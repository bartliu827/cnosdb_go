// Package ae provides the ae service.
package ae

import (
	"bufio"
	"encoding"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/pkg/network"
	"github.com/cnosdb/cnosdb/vend/cnosql"
	"github.com/cnosdb/cnosdb/vend/db/models"
	"github.com/cnosdb/cnosdb/vend/db/tsdb"
	"go.uber.org/zap"
)

const (
	// MuxHeader is the header byte used for the TCP muxer.
	MuxHeader = "aeservice"
)

// Service manages the listener for the snapshot endpoint.
type Service struct {
	wg sync.WaitGroup

	Node *meta.Node

	Interval int64

	ShardWriter interface {
		WriteShard(shardID, ownerID uint64, points []models.Point) error
	}

	MetaClient interface {
		encoding.BinaryMarshaler
		Data() meta.Data
		SetData(data *meta.Data) error
		ShardOwner(shardID uint64) (database, rp string, sgi *meta.ShardGroupInfo)
	}

	TSDBStore interface {
		Shard(id uint64) *tsdb.Shard
		WriteToShard(shardID uint64, points []models.Point) error
		ScanFiledValue(shardID uint64, key string, start, end int64, fn tsdb.ScanFiledFunc) error
	}

	Listener net.Listener
	Logger   *zap.Logger
}

// NewService returns a new instance of Service.
func NewService() *Service {
	return &Service{
		Interval: int64(5 * 60 * time.Second),
		Logger:   zap.NewNop(),
	}
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting ae service")

	s.wg.Add(1)
	go s.serve()
	return nil
}

// Close implements the Service interface.
func (s *Service) Close() error {
	if s.Listener != nil {
		if err := s.Listener.Close(); err != nil {
			return err
		}
	}
	s.wg.Wait()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "ae"))
}

func (s *Service) routineLoop() {
	for {
		data := s.MetaClient.Data()
		for _, db := range data.Databases {
			shards := db.ShardInfos()

			for _, shard := range shards {
				if len(shard.Owners) < 2 {
					continue
				}

				if shard.Owners[0].NodeID != s.Node.ID {
					continue
				}

				time.Sleep(time.Second * 3)
			}
		}

		time.Sleep(time.Second * 60)
	}
}

// serve serves ae requests from the listener.
func (s *Service) checkShard(shardID uint64, interval int64) ([]DiffShardInfo, error) {
	data := s.MetaClient.Data()
	start, end := data.ShardStartAndEndTime(shardID)
	_, _, shardInfo := data.ShardDBRetentionAndInfo(shardID)
	if len(shardInfo.Owners) < 2 {
		return nil, nil
	}

	if end > int64(time.Now().Nanosecond())-int64(5*60*time.Nanosecond) {
		end = int64(time.Now().Nanosecond()) - int64(5*60*time.Nanosecond)
	}

	if start >= end {
		return nil, nil
	}

	hashs := make([]map[string][]FieldRangeDigest, 0)
	for _, owner := range shardInfo.Owners {
		node := data.DataNode(owner.NodeID)
		if node == nil {
			return nil, fmt.Errorf("can't find node(%d) address", owner.NodeID)
		}

		if node.ID == s.Node.ID {
			digest, err := s.computeShardDigest(shardID, start, end, interval)
			if err != nil {
				return nil, err
			}
			hashs = append(hashs, digest)
		} else {
			digest, err := s.requestShardDigest(shardID, start, end, interval, node.TCPHost)
			if err != nil {
				return nil, err
			}
			hashs = append(hashs, digest)
		}
	}

	fmt.Printf("hashs:")
	fmt.Println(hashs)

	result, err := getDiffDataInfo(hashs, start, end, interval, shardID)
	if err != nil {
		return nil, err
	}

	fmt.Printf("result")
	fmt.Println(result)

	return result, nil
}

func getDiffDataInfo(hashs []map[string][]FieldRangeDigest, start, end, interval int64, shardID uint64) ([]DiffShardInfo, error) {
	// start, end
	res := make([]DiffShardInfo, 0)
	nodeCount := len(hashs)

	// key: start-end-hash-field
	// value: count
	count := make(map[string]int)
	for _, node := range hashs {
		for field, values := range node {
			for _, value := range values {
				key := fmt.Sprintf("%d-%d-%d-%s", value.StartTime, value.EndTime, value.Digest, field)
				count[key]++
			}
		}
	}
	//it's used to distinct
	rdMap := make(map[string]bool)
	for key, value := range count {
		if value != nodeCount {
			ss := strings.Split(key, "-")
			rdMap[fmt.Sprintf("%s-%s-%s", ss[0], ss[1], ss[3])] = true
		}
	}
	for key, _ := range rdMap {
		ss := strings.Split(key, "-")
		start, err := strconv.ParseInt(ss[0], 10, 64)
		if err != nil {
			return nil, err
		}
		end, _ := strconv.ParseInt(ss[1], 10, 64)
		if err != nil {
			return nil, err
		}
		field := ss[2]
		res = append(res, DiffShardInfo{
			shardID: shardID,
			start:   start,
			end:     end,
			key:     field,
		})
	}

	return res, nil
}

// serve serves snapshot requests from the listener.
func (s *Service) serve() {
	defer s.wg.Done()

	s.Logger.Info("ae service start")

	for {
		// Wait for next connection.
		conn, err := s.Listener.Accept()
		if err != nil && strings.Contains(err.Error(), "connection closed") {
			s.Logger.Info("Listener closed")
			return
		} else if err != nil {
			s.Logger.Info("Error accepting ae request", zap.Error(err))
			continue
		}

		// Handle connection in separate goroutine.
		s.wg.Add(1)
		go func(conn net.Conn) {
			defer s.wg.Done()
			defer conn.Close()
			if err := s.handleConn(conn); err != nil {
				s.Logger.Info("ae service handle conn error", zap.Error(err))
			}
		}(conn)
	}
}

// handleConn processes conn. This is run in a separate goroutine.
func (s *Service) handleConn(conn net.Conn) error {
	var typ [1]byte

	if _, err := io.ReadFull(conn, typ[:]); err != nil {
		return err
	}

	switch RequestType(typ[0]) {
	case RequestDumpFieldValues:
		return s.processDumpFieldValues(conn)

	case RequestRepairShard:
		return s.ProcessRepairShard(conn)

	case RequestShardDigests:
		return s.processShardDigests(conn)

	default:
		return fmt.Errorf("ae request type unknown: %v", typ)
	}
}

func (s *Service) requestShardDigest(shardId uint64, start, end, interval int64, addr string) (map[string][]FieldRangeDigest, error) {
	conn, err := network.Dial("tcp", addr, MuxHeader)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := &ShardDigestRequest{
		Type:      RequestShardDigests,
		StartTime: start,
		EndTime:   end,
		ShardID:   shardId,
		Interval:  interval,
	}

	_, err = conn.Write([]byte{byte(request.Type)})
	if err != nil {
		return nil, err
	}

	// Write the request
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return nil, fmt.Errorf("encode snapshot request: %s", err)
	}

	var resp ShardDigestResponse
	// Read the response
	if err := gob.NewDecoder(conn).Decode(&resp.Hash); err != nil {
		return nil, err
	}

	return resp.Hash, nil
}

func (s *Service) inspection(shardId uint64) {
	//s.shardDigest()
	//findInconsistentRange()
	//s.dumpFieldValues()
	//s.findLostFieldValues()
	//s.WriteShard()
}

func (s *Service) computeShardDigest(shardID uint64, startTime, endTime, interval int64) (map[string][]FieldRangeDigest, error) {
	resultSet := make(map[string][]FieldRangeDigest)
	for start := startTime; start < endTime; start += interval {
		end := start + interval
		if end > endTime {
			end = endTime
		}

		valcount := 0
		new64Hash := fnv.New64()
		err := s.TSDBStore.ScanFiledValue(shardID, "", start, end, func(key string, ts int64, dt cnosql.DataType, val interface{}) error {
			if ts != tsdb.EOF {
				valcount += 1
				new64Hash.Write([]byte(fmt.Sprintf("%d:%v", ts, val)))
			}

			if ts == tsdb.EOF && valcount > 0 {
				digest := FieldRangeDigest{start, end, new64Hash.Sum64()}
				resultSet[key] = append(resultSet[key], digest)

				valcount = 0
				new64Hash.Reset()
			}

			return nil
		})

		if err != nil {
			return nil, err
		}
	}

	return resultSet, nil
}

func (s *Service) dumpFieldValuesReq(key string, shardId uint64,
	start, end int64, addr string) (*FieldValueIterator, cnosql.DataType, error) {
	request := DumpFieldValuesRequest{
		Type:      RequestDumpFieldValues,
		ShardID:   shardId,
		FieldKey:  key,
		StartTime: start,
		EndTime:   end,
	}

	conn, err := network.Dial("tcp", addr, MuxHeader)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Close()

	_, err = conn.Write([]byte{byte(request.Type)})
	if err != nil {
		return nil, 0, err
	}

	// Write the request
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return nil, 0, fmt.Errorf("encode dump field values request: %s", err)
	}

	buf := bufio.NewReader(conn)
	dataByte, err := buf.ReadByte()
	if err != nil {
		return nil, 0, err
	}

	dataType := cnosql.DataType(dataByte)

	return &FieldValueIterator{reader: buf, dataType: dataType}, dataType, nil
}

func (s *Service) ProcessRepairShard(conn net.Conn) error {
	var r RepairShardRequest
	d := json.NewDecoder(conn)

	if err := d.Decode(&r); err != nil {
		return err
	}

	localAddr := s.Listener.Addr().String()
	s.Logger.Info("repair shard command ",
		zap.String("Local", localAddr),
		zap.Uint64("ShardID", r.ShardID))

	s.inspection(r.ShardID)

	io.WriteString(conn, "Repair Shard Succeeded")

	return nil
}

func (s *Service) processDumpFieldValues(conn net.Conn) error {
	req := DumpFieldValuesRequest{}
	jsonDecoder := json.NewDecoder(conn)

	if err := jsonDecoder.Decode(&req); err != nil {
		return err
	}

	alreadyWriteType := false
	buf := bufio.NewWriter(conn)
	s.TSDBStore.ScanFiledValue(req.ShardID, req.FieldKey, req.StartTime, req.EndTime,
		func(key string, ts int64, dataType cnosql.DataType, val interface{}) error {
			if !alreadyWriteType {
				if err := buf.WriteByte(byte(dataType)); err != nil {
					return err
				}
				alreadyWriteType = true
			}

			if err := binary.Write(buf, binary.BigEndian, ts); err != nil {
				return err
			}

			switch val := val.(type) {
			case bool, int64, uint64, float64:
				if err := binary.Write(buf, binary.BigEndian, val); err != nil {
					return err
				}
			case string:
				if err := binary.Write(buf, binary.BigEndian, uint32(len(val))); err != nil {
					return err
				}
				if _, err := buf.WriteString(val); err != nil {
					return err
				}

			default:
				//do nothing
			}

			return nil
		})

	return nil
}

func (s *Service) findInconsistentRange() {
}

func (s *Service) findLostFieldValues(dsis []DiffShardInfo) (err error) {
	/**
	1.向各个node发送请求，拿回数据 ✔️
	2.合并各个node的数据 ✔️
		针对每个node，shardID，生成ts,val列表 ✔️
	3.向node发送各自所缺少的[]models.Point ✔️
	*/
	data := s.MetaClient.Data()

	for _, dsi := range dsis {
		shardID := dsi.shardID
		key := dsi.key
		st := dsi.start
		et := dsi.end

		_, _, si := data.ShardDBRetentionAndInfo(shardID)
		//NodeIdMapAddr{NodeID:TCPHost}
		NodeIdMapAddr := make(map[uint64]string)

		for _, owner := range si.Owners {
			node := data.DataNode(owner.NodeID)
			if node == nil {
				return fmt.Errorf("can't find node %d", owner.NodeID)
			}
			NodeIdMapAddr[owner.NodeID] = node.TCPHost
		}

		//fvis{nodeId : *FieldValueIterator}
		fvis := make(map[uint64]*FieldValueIterator)
		//dts{nodeId: cnosql.DataType
		dts := make(map[uint64]cnosql.DataType)
		for nid, addr := range NodeIdMapAddr {
			fvi, dT, err := s.dumpFieldValuesReq(key, shardID, st, et, addr)
			if err != nil {
				return err
			}
			fvis[nid] = fvi
			dts[nid] = dT
		}

		//lostData{nodeId : []tsvTuple}
		lostData := make(map[uint64][]tsvTuple)

		//tmp keep ts and val for find the min ts
		tmp := make(map[uint64]tsvTuple)

		//initialize tmp
		for nid, iter := range fvis {
			ts, val, err := iter.Next()
			if err != nil {
				return err
			}
			tmp[nid] = tsvTuple{ts, val}
		}
		for {
			//find the min ts
			mints := int64(math.MaxInt64)
			for _, v := range tmp {
				if v.ts != tsdb.EOF && mints > v.ts {
					mints = v.ts
				}
			}
			if mints == int64(math.MaxInt64) {
				break
			}
			for nid, v := range tmp {
				if v.ts == tsdb.EOF {
					continue
				}
				if v.ts == mints {
					//not lost, go next
					ts, val, err := fvis[nid].Next()
					if err != nil {
						return err
					}
					tmp[nid] = tsvTuple{ts, val}
				} else {
					//lost,add to LostData
					lostData[nid] = append(lostData[nid], tsvTuple{v.ts, v.val})
					if len(lostData[nid]) >= 500 {
						//send the lost data to node
						err := s.sendLostData(shardID, nid, key, lostData[nid], dts[nid])
						if err != nil {
							return err
						}
						lostData[nid] = make([]tsvTuple, 0)
					}
				}
			}

		}
		//send all data finally
		for nid, v := range lostData {
			err := s.sendLostData(shardID, nid, key, v, dts[nid])
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (s *Service) sendLostData(shardID, NodeID uint64, key string, lostData []tsvTuple, dataType cnosql.DataType) (err error) {
	//air,host=h1,station=XiaoMaiDao#~#visibility
	key += "="
	var buf strings.Builder
	for _, tsv := range lostData {
		val, _ := interface2String(tsv.val, dataType)
		buf.WriteString(key)
		buf.WriteString(val)
		buf.WriteString(" ")
		buf.WriteString(strconv.FormatInt(tsv.ts, 10))
		buf.WriteString("\n")
	}

	points, err := models.ParsePoints([]byte(buf.String()))
	if err != nil {
		return err
	}
	if points != nil {
		err := s.WriteShard(shardID, NodeID, points)
		if err != nil {
			return err
		}
	}
	return nil
}

func interface2String(value interface{}, dataType cnosql.DataType) (string, error) {
	switch dataType {
	case cnosql.Boolean:
		val := value.(bool)
		return strconv.FormatBool(val), nil
	case cnosql.Integer:
		var val int64
		val = value.(int64)
		return strconv.FormatInt(val, 10), nil
	case cnosql.Unsigned:
		var val uint64
		val = value.(uint64)
		return strconv.FormatUint(val, 10), nil
	case cnosql.Float:
		var val float64
		val = value.(float64)
		return strconv.FormatFloat(val, 'E', -1, 64), nil
	case cnosql.String:
		return value.(string), nil
	default:
		return "", fmt.Errorf("Unknow data type %d", dataType)
	}

}

func (s *Service) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	if ownerID != s.Node.ID {
		return s.WriteShard(shardID, ownerID, points)
	} else {
		return s.TSDBStore.WriteToShard(shardID, points)
	}
}

func (s *Service) processShardDigests(conn net.Conn) error {
	req := ShardDigestRequest{}
	jsonDecoder := json.NewDecoder(conn)
	if err := jsonDecoder.Decode(&req); err != nil {
		return err
	}

	s.Logger.Info("get the request msg")
	//get shard Digest
	resultSet, err := s.computeShardDigest(req.ShardID, req.StartTime, req.EndTime, req.Interval)
	if err != nil {
		return err
	}

	s.Logger.Info("send the shard digest back")

	if err := gob.NewEncoder(conn).Encode(resultSet); err != nil {
		return fmt.Errorf("encode resonse: %s", err.Error())
	}

	return nil
}

type FieldValueIterator struct {
	reader   *bufio.Reader
	dataType cnosql.DataType
}

func (it *FieldValueIterator) Next() (int64, interface{}, error) {
	var ts int64
	if err := binary.Read(it.reader, binary.BigEndian, &ts); err != nil {
		return 0, 0, err
	}

	switch it.dataType {
	case cnosql.Boolean:
		var val bool
		if err := binary.Read(it.reader, binary.BigEndian, &val); err != nil {
			return 0, 0, err
		}
		return ts, val, nil
	case cnosql.Integer:
		var val int64
		if err := binary.Read(it.reader, binary.BigEndian, &val); err != nil {
			return 0, 0, err
		}
		return ts, val, nil
	case cnosql.Unsigned:
		var val uint64
		if err := binary.Read(it.reader, binary.BigEndian, &val); err != nil {
			return 0, 0, err
		}
		return ts, val, nil
	case cnosql.Float:
		var val float64
		if err := binary.Read(it.reader, binary.BigEndian, &val); err != nil {
			return 0, 0, err
		}
		return ts, val, nil
	case cnosql.String:
		var len uint32
		if err := binary.Read(it.reader, binary.BigEndian, &len); err != nil {
			return 0, 0, err
		}

		bytes := make([]byte, len)
		if _, err := io.ReadFull(it.reader, bytes); err != nil {
			return 0, 0, err
		}
		return ts, string(bytes), nil
	default:
		return 0, 0, fmt.Errorf("Unknow data type %d", it.dataType)
	}
}

// RequestType indicates the typeof ae request.
type RequestType uint8

const (
	Requestxxxxxxx RequestType = iota
	RequestDumpFieldValues

	RequestRepairShard
	RequestShardDigests
)

type DiffShardInfo struct {
	key     string
	start   int64
	end     int64
	shardID uint64
}

type RepairShardRequest struct {
	Type    RequestType
	ShardID uint64
}

type FieldRangeDigest struct {
	StartTime int64
	EndTime   int64
	Digest    uint64
}

type ShardDigestRequest struct {
	Type      RequestType
	ShardID   uint64
	Interval  int64
	StartTime int64
	EndTime   int64
}

type ShardDigestResponse struct {
	Hash map[string][]FieldRangeDigest
}

type DumpFieldValuesRequest struct {
	Type      RequestType
	ShardID   uint64
	FieldKey  string
	StartTime int64
	EndTime   int64
}

type DumpFieldValuesResponse struct {
}

type NodeShardLostPoints struct {
	ownerID uint64
	shardID uint64
	points  []models.Point
}

type tsvTuple struct {
	ts  int64
	val interface{}
}

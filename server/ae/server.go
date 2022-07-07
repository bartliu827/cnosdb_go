// Package ae provides the ae service.
package ae

import (
	"bufio"
	"encoding"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cnosdb/cnosdb/pkg/network"
	"hash/fnv"
	"io"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cnosdb/cnosdb/meta"
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

type DiffShardInfo struct {
	key     string
	start   int64
	end     int64
	shardID uint64
}

// serve serves ae requests from the listener.
func (s *Service) Check(shardID uint64, interval int64) ([]DiffShardInfo, error) {
	//s.Logger.Info("1111")
	data := s.MetaClient.Data()
	_, _, si := data.ShardDBRetentionAndInfo(shardID)
	nodeList := make([]uint64, 0)
	//get NodeIDs
	for _, owner := range si.Owners {
		nodeList = append(nodeList, owner.NodeID)
	}
	//get node_address
	nodeAddrList := make([]string, 0)
	for _, nid := range nodeList {
		nodeAddrList = append(nodeAddrList, data.DataNode(nid).TCPHost)
	}
	//get shard startTime, endTime, and their hash values
	//localHash := make([]uint64, 0)
	_, _, sg := s.MetaClient.ShardOwner(shardID)
	start := sg.StartTime.UnixNano()
	end := sg.EndTime.UnixNano()

	s.Logger.Info(fmt.Sprintf("%d: %d", start, end))

	//s.Logger.Error("22222")

	hashs := make([]map[string][]FieldRangeDigest, 0)
	for _, addr := range nodeAddrList {
		conn, err := network.Dial("tcp", addr, MuxHeader)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		request := &ShardDigestRequest{
			Type:     RequestShardDigests,
			ShardID:  shardID,
			Interval: interval,
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
		if err := gob.NewDecoder(conn).Decode(&resp); err != nil {
			return nil, err
		}

	}
	fmt.Printf("hashs:")
	fmt.Println(hashs)

	//validate the hash values, find out the diff, add the time duration into result
	if len(hashs) == 0 {
		return nil, errors.New("the ShardDigest what it got in all nodes are nil ")
	}

	result := getDiffDataInfo(hashs, start, end, interval, shardID)

	fmt.Printf("result")
	fmt.Println(result)

	return result, nil
}

func getDiffDataInfo(hashs []map[string][]FieldRangeDigest, start, end, interval int64, shardID uint64) []DiffShardInfo {
	// start, end
	//aa
	res := make([]DiffShardInfo, 0)
	nodeCount := len(hashs)
	fieldMap := make(map[string]int)
	for _, node := range hashs {
		for key, _ := range node {
			fieldMap[key]++
		}
	}
	// count, hash. isWrong
	record := make(map[string][][]uint64, len(fieldMap))
	for field, _ := range fieldMap {
		record[field] = make([][]uint64, (end-start+interval-1)/interval)
		for i := 0; i < len(record[field]); i++ {
			record[field][i] = []uint64{0, 0, 0}
		}
		for j := 0; j < nodeCount; j++ {
			for k := 0; k < len(hashs[j][field]); k++ {
				idx := (hashs[j][field][k].StartTime - start) / interval
				if _, ok := hashs[j][field]; !ok {
					//if res[field] == nil {
					//	res[field] = make([][]int64, 0)
					//}
					//res[field] = append(res[field], []int64{startTime, endTime})
					record[field][idx][2] = 1
					break
				}
				if hashs[j][field][idx].StartTime == (start + idx*interval) {
					if record[field][idx][2] == 1 || (record[field][idx][1] != 0 && record[field][idx][1] != hashs[j][field][idx].Digest) {
						record[field][idx][2] = 1
						break
					}
					record[field][idx][0]++
					record[field][idx][1] = hashs[j][field][idx].Digest
				}
			}
		}
	}

	for field, values := range record {
		for i := 0; i < len(values); i++ {
			if record[field][i][2] == 1 {
				startTime := start + int64(i)*interval
				endTime := startTime + interval
				if endTime > end {
					endTime = end
				}
				res = append(res, DiffShardInfo{
					shardID: shardID,
					key:     field,
					start:   startTime,
					end:     endTime,
				})
			}
		}
	}

	return res
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

	case RequestShardDigests:
		return s.processShardDigests(conn)

	default:
		return fmt.Errorf("ae request type unknown: %v", typ)
	}
}

func (s *Service) routineLoop() {
	for {
		time.Sleep(time.Second * 1)
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

				for _, owner := range shard.Owners {
					addr := data.DataNode(owner.NodeID).TCPHost
					s.shardDigest(shard.ID, math.MinInt64, math.MaxInt64, s.Interval, addr)
				}

				time.Sleep(time.Second * 3)
			}
		}
	}
}

func (s *Service) shardDigest(shardId uint64, start, end, interval int64, addr string) (map[string][]FieldRangeDigest, error) {
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

	for i := startTime; i < endTime; i += interval {
		st := i
		et := i + interval
		if et > endTime {
			et = endTime
		}
		keyTV := make(map[string][]byte)
		err := s.TSDBStore.ScanFiledValue(shardID, "", st, et, func(key string, ts int64, dt cnosql.DataType, val interface{}) error {
			var item []byte
			item = append(item, []byte(fmt.Sprintf("%d", ts))...)
			item = append(item, []byte(fmt.Sprintf("%v", val))...)

			keyTV[key] = append(keyTV[key], item...)
			return nil
		})
		for k, v := range keyTV {
			new64 := fnv.New64()
			new64.Write([]byte(k))
			new64.Write(v)
			sum64 := new64.Sum64()
			resultSet[k] = append(resultSet[k], FieldRangeDigest{st, et, sum64})
		}
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

func (s *Service) findLostFieldValues(dsis []DiffShardInfo) (result []NodeShardLostPoints) {
	//1.向各个node发送请求，拿回数据 ✅
	//2.合并各个node的数据
	//3.对于每个node，返回它所缺少的[]models.Point
	data := s.MetaClient.Data()

	for _, dsi := range dsis {
		shardID := dsi.shardID
		_, _, si := data.ShardDBRetentionAndInfo(shardID)
		nodeList := make([]uint64, 0)
		//get NodeIDs
		for _, owner := range si.Owners {
			nodeList = append(nodeList, owner.NodeID)
		}
		//get node_address
		nodeAddrList := make([]string, 0)
		for _, nid := range nodeList {
			nodeAddrList = append(nodeAddrList, data.DataNode(nid).TCPHost)
		}

		//var fvis []*FieldValueIterator
		//for _, addr := range nodeAddrList {
		//	fvi, dType, err := s.dumpFieldValuesReq(dsi.key, dsi.shardID, dsi.start, dsi.end, addr)
		//	if err != nil {
		//		return nil
		//	}
		//
		//	fvis = append(fvis, fvi)
		//}
		//var heap Int64Heap
		//for idx, iter := range fvis {
		//	ts, v, err2 := iter.Next()
		//	if err2 != nil {
		//		return nil
		//	}
		//
		//	heap
		//
		//}

	}

	return nil
}

func (s *Service) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	if ownerID != s.Node.ID {
		return s.WriteShard(shardID, ownerID, points)
	} else {
		return s.TSDBStore.WriteToShard(shardID, points)
	}
}

func (s *Service) digest_example() {
	models.ParsePointsString("")
	//air,host=h1,station=XiaoMaiDao#~#visibility
	file, _ := os.Create("./scan_field")
	defer file.Close()
	s.TSDBStore.ScanFiledValue(36, "", 1555144313000000000, 1755144315000000000,
		func(key string, ts int64, ty cnosql.DataType, val interface{}) error {
			file.WriteString(fmt.Sprintf("%s %d %v\n", key, ts, val))
			//fmt.Printf("========ScanFiledValue %s %d %v\n", key, ts, val)
			return nil
		})

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

	RequestGetDiffData
	RequestShardDigests
)

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
	shardID uint64
	ownerID uint64
	points  []models.Point
}

func (s *Service) getDataByTime(conn net.Conn, key string, shardID uint64, start, end int64) error {

	res := DumpFieldValuesResponse{}
	shard := s.TSDBStore.Shard(shardID)
	//file, err := os.Create("./diffData")
	//if err != nil {
	//	return err
	//}
	//defer file.Close()
	//bw := bufio.NewWriterSize(file, 1024*1024)
	//defer bw.Flush()
	//var w io.Writer = bw

	var fn func(key string, ts int64, ty cnosql.DataType, val interface{}) error
	fn = func(key string, ts int64, ty cnosql.DataType, val interface{}) error {
		//if _, err := w.Write([]byte(fmt.Sprintf("%v", val))); err != nil {
		//	return err
		//}
		return nil
	}

	if err := shard.ScanFiledValue(key, start, end, fn); err != nil {
		return err
	}

	if err := json.NewEncoder(conn).Encode(res); err != nil {
		return fmt.Errorf("encode resonse: %s", err.Error())
	}

	return nil
}

func (s *Service) GetDiffData(shardID uint64, key string, startTime, endTime int64) error {
	data := s.MetaClient.Data()
	_, _, si := data.ShardDBRetentionAndInfo(shardID)
	nodeList := make([]uint64, 0)
	//get NodeIDs
	for _, owner := range si.Owners {
		nodeList = append(nodeList, owner.NodeID)
	}
	//get node_address
	nodeAddrList := make([]string, 0)
	for _, nid := range nodeList {
		nodeAddrList = append(nodeAddrList, data.DataNode(nid).TCPHost)
	}

	for _, addr := range nodeAddrList {
		conn, err := network.Dial("tcp", addr, MuxHeader)
		if err != nil {
			return err
		}
		defer conn.Close()

		request := &DumpFieldValuesRequest{
			Type:      RequestGetDiffData,
			ShardID:   shardID,
			FieldKey:  key,
			StartTime: startTime,
			EndTime:   endTime,
		}

		_, err = conn.Write([]byte{byte(request.Type)})
		if err != nil {
			return err
		}

		// Write the request
		if err := json.NewEncoder(conn).Encode(request); err != nil {
			return fmt.Errorf("encode ae request: %s", err)
		}

		var resp DumpFieldValuesResponse
		// Read the response
		if err := json.NewDecoder(conn).Decode(&resp); err != nil {
			return err
		}
	}
	return nil
}

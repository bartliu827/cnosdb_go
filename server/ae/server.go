// Package ae provides the ae service.
package ae

import (
	"bufio"
	"encoding"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/cnosdb/cnosdb/pkg/network"

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
		Logger: zap.NewNop(),
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

// serve serves ae requests from the listener.
func (s *Service) Check(shardID uint64, interval int64) ([][]int64, error) {
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

	hashs := make([][]int64, 0)
	for _, addr := range nodeAddrList {
		conn, err := network.Dial("tcp", addr, MuxHeader)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		request := &ShardDigestRequest{
			Type:     RequestShardIntervalHash,
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
		if err := json.NewDecoder(conn).Decode(&resp); err != nil {
			return nil, err
		}

		hashs = append(hashs, resp.Hash)
	}
	fmt.Printf("hashs:")
	fmt.Println(hashs)

	result := make([][]int64, 0)
	//validate the hash values, find out the diff, add the time duration into result
	if hashs == nil {
		return nil, nil
	}
	for j := 0; j < len(hashs[0]); j++ {
		tmp := hashs[0][j]
		for i := 0; i < len(hashs); i++ {
			if tmp != hashs[i][j] {
				st := start + int64(j)*interval
				et := st + interval
				result = append(result, []int64{st, et})
				continue
			}
		}
	}
	fmt.Printf("result")
	fmt.Println(result)

	return result, nil
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
	case Requestxxxxxxx:

	default:
		return fmt.Errorf("ae request type unknown: %v", typ)
	}

	return nil
}

func (s *Service) routineLoop() {
	//s.inspection(123)
}

func (s *Service) inspection(shardId uint64) {
	//s.shardDigest()
	//findInconsistentRange()
	//s.dumpFieldValues()
	//s.findLostFieldValues()
	//s.WriteShard()
}

func (s *Service) shardDigest(shardId uint64, start, end, interval int64, addr string) {
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

func (s *Service) findLostFieldValues() {
}

func (s *Service) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	if ownerID != s.Node.ID {
		return s.WriteShard(shardID, ownerID, points)
	} else {
		return s.TSDBStore.WriteToShard(shardID, points)
	}
}

func (s *Service) digest_example() {
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

func (s *Service) getShardIntervalHash(conn net.Conn, shardID uint64, interval int64) error {
	s.Logger.Info("get the request msg")

	resp := ShardDigestResponse{}
	//获取shqrd段的hash值
	//data := s.MetaClient.Data()

	_, _, sg := s.MetaClient.ShardOwner(shardID)
	start := sg.StartTime.UnixNano()
	end := sg.EndTime.UnixNano()

	//name := RandomString(4)
	//var resultSet map[string][]int64
	for i := start; i < end; i += interval {
		//TODO: get data from shard and compute their hash value

		//fnv64Hash := fnv.New64()
		//fnv64Hash.Write(d)
		//h := fnv64Hash.Sum64()
		//res.Hash = append(res.Hash, h)
	}
	s.Logger.Info("return the hash value")
	//s.Logger.Info(fmt.Sprint(res))
	if err := json.NewEncoder(conn).Encode(resp); err != nil {
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
	RequestShardIntervalHash
)

type FieldRangeDigest struct {
	StartTime int64
	EndTime   int64
	Digest    string
}

type ShardDigestRequest struct {
	Type      RequestType
	ShardID   uint64
	Interval  int64
	StartTime int64
	EndTime   int64
}

type ShardDigestResponse struct {
	Hash []int64
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

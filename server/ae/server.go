// Package ae provides the ae service.
package ae

import (
	"encoding"
	"encoding/json"
	"fmt"
	"github.com/cnosdb/cnosdb/pkg/network"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/cnosdb/cnosdb/meta"
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

func (s *Service) WriteShard(shardID, ownerID uint64, points []models.Point) error {
	if ownerID != s.Node.ID {
		return s.WriteShard(shardID, ownerID, points)
	} else {
		return s.TSDBStore.WriteToShard(shardID, points)
	}
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

// RequestType indicates the typeof ae request.
type RequestType uint8

const (
	Requestxxxxxxx RequestType = iota

	RequestShardIntervalHash
)

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

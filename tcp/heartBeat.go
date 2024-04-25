package tcp

import (
	"fmt"
	"io"
	"net"
)

type Server struct {
	address string
	ln      net.Listener
	quitch  chan struct{}
}

func NewServer(address string) *Server {
	return &Server{
		address: address,
		quitch:  make(chan struct{}),
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	defer ln.Close()

	fmt.Println("TCP STARTED")
	s.ln = ln
	go s.acceptLoop()

	<-s.quitch

	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			fmt.Println("accept error: ", err)
			continue
		}
		go s.readLoop(conn)
	}
}

func (s *Server) readLoop(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 2048)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
				return
			}
			fmt.Println("read error: ", err)
			return
		}
		msg := buf[:n]
		fmt.Println(string(msg))
	}
}

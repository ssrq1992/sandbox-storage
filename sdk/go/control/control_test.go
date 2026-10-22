package control

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMTLSPeerAndVersionFailClosed(t *testing.T) {
	directory := t.TempDir()
	key, e := rsa.GenerateKey(rand.Reader, 2048)
	if e != nil {
		t.Fatal(e)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, e := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	caFile := filepath.Join(directory, "ca.pem")
	_ = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600)
	leaf := func(name, identity string, serial int64) Config {
		leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		uri, _ := url.Parse(identity)
		cert := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{uri}}
		b, err := x509.CreateCertificate(rand.Reader, cert, ca, &leafKey.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		cfg := Config{Cert: filepath.Join(directory, name+".pem"), Key: filepath.Join(directory, name+".key"), CA: caFile}
		_ = os.WriteFile(cfg.Cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: b}), 0600)
		_ = os.WriteFile(cfg.Key, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(leafKey)}), 0600)
		return cfg
	}
	serverConfig := leaf("coordinator", "spiffe://coordinator", 2)
	clientConfig := leaf("scheduler", "spiffe://scheduler", 3)
	calls := 0
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !HasPeer(r.TLS, []string{"spiffe://scheduler"}) {
			http.Error(w, "denied", 403)
			return
		}
		q, err := Decode(w, r)
		if err != nil {
			Write(w, Response{}, err)
			return
		}
		calls++
		Write(w, Response{LaunchID: q.LaunchID}, nil)
	}))
	server.TLS, e = TLS(serverConfig, true)
	if e != nil {
		t.Fatal(e)
	}
	server.StartTLS()
	defer server.Close()
	clientConfig.Endpoint = server.URL
	clientConfig.Peer = "spiffe://coordinator"
	client, e := New(clientConfig)
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	response, e := client.Call(context.Background(), "InspectOperation", Request{Runtime: "agentenv", LaunchID: "unchanged"})
	if e != nil || response.LaunchID != "unchanged" {
		t.Fatalf("mTLS call failed %+v %v", response, e)
	}
	clientConfig.Peer = "spiffe://wrong"
	bad, e := New(clientConfig)
	if e != nil {
		t.Fatal(e)
	}
	defer bad.Close()
	if _, e = bad.Call(context.Background(), "InspectOperation", Request{}); e == nil {
		t.Fatal("accepted wrong verified server URI")
	}
	if calls != 1 {
		t.Fatal("unauthorized call reached handler")
	}
	clientConfig.Endpoint = "http://127.0.0.1"
	if _, e = New(clientConfig); e == nil {
		t.Fatal("plaintext accepted")
	}
}

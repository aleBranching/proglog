package config

import (
	"os"
	"path"
)

var (
	CaFile               = configFile("ca.pem")
	ServerCertFile       = configFile("server.pem")
	ServerKeyFile        = configFile("server-key.pem")
	RootClientCertFile   = configFile("root-client.pem")
	RootClientKeyFile    = configFile("root-client-key.pem")
	NobodyClientCertFile = configFile("nobody-client.pem")
	NobodyClientKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile         = configFile("model.conf")
	ACLPolicyFile        = configFile("policy.csv")
)

func configFile(filename string) string {
	// Watch out. The makefile sets a static dir for the certs. at $HOME
	if confDirENV := os.Getenv("CONFIG_DIR"); confDirENV != "" {
		return path.Join(confDirENV, filename)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic("something seriously wrong" + err.Error())
	}

	return path.Join(homeDir, ".proglog", filename)

}

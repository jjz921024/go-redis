package redis

import (
	"encoding/hex"
	"errors"

	"github.com/wenzhenxi/gorsa"
)

// decrypt
func pubKeyDecrypt(engypted, pubKey string) ([]byte, error) {
	out, err := hex.DecodeString(engypted)
	if err != nil {
		return nil, errors.New("hex decode error:" + err.Error())
	}
	grsa := gorsa.RSASecurity{}
	err = grsa.SetPublicKey(pubKey)
	if err != nil {
		return nil, errors.New("set public key error:" + err.Error())
	}
	rsadata, err := grsa.PubKeyDECRYPT(out)
	if err != nil {
		return nil, errors.New("public key decrypt error:" + err.Error())
	}
	return rsadata, nil
}

func priKeyDecrypt(rsadata []byte, privKey string) ([]byte, error) {
	grsa := gorsa.RSASecurity{}
	err := grsa.SetPrivateKey(privKey)
	if err != nil {
		return nil, errors.New("set private key error:" + err.Error())
	}
	rsadata2, err := grsa.PriKeyDECRYPT(rsadata)
	if err != nil {
		return nil, errors.New("private key decrypt error:" + err.Error())
	}
	return rsadata2, nil
}

func publicKeyEncrypt(data, pubKey string) (string, error) {
	grsa := gorsa.RSASecurity{}
	err := grsa.SetPublicKey(pubKey)
	if err != nil {
		return "", errors.New("set public key error:" + err.Error())
	}

	b, err := grsa.PubKeyENCTYPT([]byte(data))
	if err != nil {
		return "", errors.New("public key encrypt error:" + err.Error())
	}
	return hex.EncodeToString(b), nil
}

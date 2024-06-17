package conf

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"time"
)

type NodeIdData struct {
	NodeIdAddress string `json:"node_id"`
	NodeName      string `json:"node_name"`
}

type ServerSignals struct {
	StationName string       `json:"station_name"`
	OPCUAddress string       `json:"opcuaddress"`
	NodeIds     []NodeIdData `json:"nodeids"`
}

type Conf struct {
	BootstrapServers      string `json:"bootstrap_servers"`
	SignalsFilename       string `json:"signals_filename"`
	PublishingTopicname   string `json:"publishing_topicname"`
	ServersData           []ServerSignals
	CleaningThresholdMsec int64 `json:"cleaning_threshold_msec"`
	PullRateMsec          int64 `json:"pull_rate_msec"`
	PubRateMsec           int64 `json:"pub_rate_msec"`
	cleaning_threshold    time.Duration
	pull_rate             time.Duration
	pub_rate              time.Duration
}

func NewConf(filepath string) (*Conf, error) {
	var conf *Conf = new(Conf)

	conf_file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer conf_file.Close()

	json_bytes, err := ioutil.ReadAll(conf_file)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(json_bytes, conf)
	if err != nil {
		return nil, err
	}

	conf.cleaning_threshold = time.Duration(conf.CleaningThresholdMsec * int64(time.Millisecond))
	conf.pull_rate = time.Duration(conf.PullRateMsec * int64(time.Millisecond))
	conf.pub_rate = time.Duration(conf.PubRateMsec * int64(time.Millisecond))

	conf.ServersData = []ServerSignals{}

	//I then proceed to obtain the servers data
	signals_file, err := os.Open(conf.SignalsFilename)
	if err != nil {
		return nil, err
	}
	defer signals_file.Close()

	json_bytes, err = ioutil.ReadAll(signals_file)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(json_bytes, &conf.ServersData)
	if err != nil {
		return nil, err
	}

	return conf, nil
}

func (c *Conf) GetCleaningThreshold() time.Duration {
	return c.cleaning_threshold
}

func (c *Conf) GetPullRate() time.Duration {
	return c.pull_rate
}

func (c *Conf) GetPubRate() time.Duration {
	return c.pub_rate
}

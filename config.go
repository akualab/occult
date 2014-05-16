package occult

import (
	"io/ioutil"
	"log"

	"launchpad.net/goyaml"
)

/*
Configuration file. Example:

   app:
     name: "myapp"
     cache_cap: 1000
   cluster:
     nodes:
       - id: 0
       - addr: ":33330"
       - id: 1
       - addr: ":33331"
*/
type Config struct {
	App     *App     `yaml:"app"`
	Cluster *Cluster `yaml:"cluster"`
}

// Read the occult configuration file.
func ReadConfig(filename string) (config *Config, err error) {

	var data []byte
	data, err = ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	config = &Config{}
	err = goyaml.Unmarshal(data, config)
	if err != nil {
		return
	}
	//log.Printf("config:\n%s\n\n", config)

	return
}

func OneNodeConfig() (config *Config) {
	return &Config{App: &App{Name: "eval", CacheCap: 1000}}
}

func (c *Config) String() string {

	d, err := goyaml.Marshal(c)
	if err != nil {
		log.Fatal(err)
	}
	return string(d)
}

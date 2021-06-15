package gokong

import (
	"encoding/json"
	"fmt"
)

type ServiceClient interface {
	Create(serviceRequest *ServiceRequest) (*Service, error)
	GetServiceByName(name string) (*Service, error)
	GetServiceById(id string) (*Service, error)
	GetServiceFromRouteId(id string) (*Service, error)
	GetServices(query *ServiceQueryString) ([]*Service, error)
	UpdateServiceByName(name string, serviceRequest *ServiceRequest) (*Service, error)
	UpdateServiceById(id string, serviceRequest *ServiceRequest) (*Service, error)
	UpdateServicebyRouteId(id string, serviceRequest *ServiceRequest) (*Service, error)
	DeleteServiceByName(name string) error
	DeleteServiceById(id string) error
	CreatePluginConfig(serviceId string, pluginName string, pluginConfig string) (*ServicePluginConfig, error)
	GetPluginConfig(serviceId string, pluginName string, id string) (*ServicePluginConfig, error)
	GetPluginConfigs(serviceId string, pluginName string) ([]map[string]interface{}, error)
	DeletePluginConfig(serviceId string, pluginName string, id string) error
}

type serviceClient struct {
	config *Config
}

type ServiceRequest struct {
	Name           *string   `json:"name" yaml:"name"`
	Protocol       *string   `json:"protocol" yaml:"protocol"`
	Host           *string   `json:"host" yaml:"host"`
	Port           *int      `json:"port,omitempty" yaml:"port,omitempty"`
	Path           *string   `json:"path,omitempty" yaml:"path,omitempty"`
	Retries        *int      `json:"retries,omitempty" yaml:"retries,omitempty"`
	ConnectTimeout *int      `json:"connect_timeout,omitempty" yaml:"connect_timeout,omitempty"`
	WriteTimeout   *int      `json:"write_timeout,omitempty" yaml:"write_timeout,omitempty"`
	ReadTimeout    *int      `json:"read_timeout,omitempty" yaml:"read_timeout,omitempty"`
	Url            *string   `json:"url,omitempty" yaml:"url,omitempty"`
	Tags           []*string `json:"tags,omitempty" yaml:"tags,omitempty"`
}

type Service struct {
	Id             *string   `json:"id" yaml:"id"`
	CreatedAt      *int      `json:"created_at" yaml:"created_at"`
	UpdatedAt      *int      `json:"updated_at" yaml:"updated_at"`
	Protocol       *string   `json:"protocol" yaml:"protocol"`
	Host           *string   `json:"host" yaml:"host"`
	Port           *int      `json:"port" yaml:"port"`
	Path           *string   `json:"path" yaml:"path"`
	Name           *string   `json:"name" yaml:"name"`
	Retries        *int      `json:"retries" yaml:"retries"`
	ConnectTimeout *int      `json:"connect_timeout" yaml:"connect_timeout"`
	WriteTimeout   *int      `json:"write_timeout" yaml:"write_timeout"`
	ReadTimeout    *int      `json:"read_timeout" yaml:"read_timeout"`
	Url            *string   `json:"url" yaml:"url"`
	Tags           []*string `json:"tags,omitempty" yaml:"tags,omitempty"`
}

type Services struct {
	Data   []*Service `json:"data" yaml:"data"`
	Next   *string    `json:"next" yaml:"mext"`
	Offset string     `json:"offset,omitempty" yaml:"offset,omitempty"`
}

type ServiceQueryString struct {
	Offset string `json:"offset,omitempty"`
	Size   int    `json:"size"`
}

type ServicePluginConfig struct {
	Id   string `json:"id,omitempty" yaml:"id,omitempty"`
	Body string
}

type ServicePluginConfigs struct {
	Data   []map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`
	Next   string                   `json:"next,omitempty" yaml:"next,omitempty"`
	Offset string                   `json:"offset,omitempty" yaml:"offset,omitempty"`
}

const ServicesPath = "/services/"

func (serviceClient *serviceClient) Create(serviceRequest *ServiceRequest) (*Service, error) {
	if serviceRequest.Port == nil {
		serviceRequest.Port = Int(80)
	}

	if serviceRequest.Retries == nil {
		serviceRequest.Retries = Int(5)
	}

	if serviceRequest.ConnectTimeout == nil {
		serviceRequest.ConnectTimeout = Int(60000)
	}

	if serviceRequest.ReadTimeout == nil {
		serviceRequest.ReadTimeout = Int(60000)
	}

	if serviceRequest.WriteTimeout == nil {
		serviceRequest.WriteTimeout = Int(60000)
	}

	r, body, errs := newPost(serviceClient.config, ServicesPath).Send(serviceRequest).End()
	if errs != nil {
		return nil, fmt.Errorf("could not register the service, error: %v", errs)
	}

	if r.StatusCode == 401 || r.StatusCode == 403 {
		return nil, fmt.Errorf("not authorised, message from kong: %s", body)
	}

	if r.StatusCode == 400 {
		return nil, fmt.Errorf("bad request, message from kong: %s", body)
	}

	createdService := &Service{}
	err := json.Unmarshal([]byte(body), createdService)
	if err != nil {
		return nil, fmt.Errorf("could not parse service get response, error: %v", err)
	}

	if createdService.Id == nil {
		return nil, fmt.Errorf("could not register the service, error: %v", body)
	}

	return createdService, nil
}

func (serviceClient *serviceClient) GetServiceByName(name string) (*Service, error) {
	return serviceClient.GetServiceById(name)
}

func (serviceClient *serviceClient) GetServiceById(id string) (*Service, error) {
	return serviceClient.getService(ServicesPath + id)
}

func (serviceClient *serviceClient) GetServiceFromRouteId(id string) (*Service, error) {
	return serviceClient.getService("/routes/" + id + "/service")
}

func (serviceClient *serviceClient) getService(endpoint string) (*Service, error) {
	r, body, errs := newGet(serviceClient.config, endpoint).End()
	if errs != nil {
		return nil, fmt.Errorf("could not get the service, error: %v", errs)
	}

	if r.StatusCode == 401 || r.StatusCode == 403 {
		return nil, fmt.Errorf("not authorised, message from kong: %s", body)
	}

	service := &Service{}
	err := json.Unmarshal([]byte(body), service)
	if err != nil {
		return nil, fmt.Errorf("could not parse service get response, error: %v", err)
	}

	if service.Id == nil {
		return nil, nil
	}

	return service, nil
}

func (serviceClient *serviceClient) GetServices(query *ServiceQueryString) ([]*Service, error) {
	services := make([]*Service, 0)

	if query.Size == 0 || query.Size < 100 {
		query.Size = 100
	}

	if query.Size > 1000 {
		query.Size = 1000
	}

	for {
		data := &Services{}

		r, body, errs := newGet(serviceClient.config, ServicesPath).Query(*query).End()
		if errs != nil {
			return nil, fmt.Errorf("could not get the service, error: %v", errs)
		}

		if r.StatusCode == 401 || r.StatusCode == 403 {
			return nil, fmt.Errorf("not authorised, message from kong: %s", body)
		}

		err := json.Unmarshal([]byte(body), data)
		if err != nil {
			return nil, fmt.Errorf("could not parse service get response, error: %v", err)
		}

		services = append(services, data.Data...)

		if data.Next == nil || *data.Next == "" {
			break
		}

		query.Offset = data.Offset
	}

	return services, nil
}

func (serviceClient *serviceClient) UpdateServiceByName(name string, serviceRequest *ServiceRequest) (*Service, error) {
	return serviceClient.UpdateServiceById(name, serviceRequest)
}

func (serviceClient *serviceClient) UpdateServiceById(id string, serviceRequest *ServiceRequest) (*Service, error) {
	return serviceClient.updateService(ServicesPath+id, serviceRequest)
}

func (serviceClient *serviceClient) UpdateServicebyRouteId(id string, serviceRequest *ServiceRequest) (*Service, error) {
	return serviceClient.updateService("/routes/"+id+"/service", serviceRequest)
}

func (serviceClient *serviceClient) DeleteServiceByName(name string) error {
	return serviceClient.DeleteServiceById(name)
}

func (serviceClient *serviceClient) DeleteServiceById(id string) error {
	r, body, errs := newDelete(serviceClient.config, ServicesPath+id).End()
	if errs != nil {
		return fmt.Errorf("could not delete the service, result: %v error: %v", r, errs)
	}

	if r.StatusCode == 400 {
		return fmt.Errorf("bad request, message from kong: %s", body)
	}

	if r.StatusCode == 401 || r.StatusCode == 403 {
		return fmt.Errorf("not authorised, message from kong: %s", body)
	}

	return nil
}

func (serviceClient *serviceClient) updateService(requestPath string, serviceRequest *ServiceRequest) (*Service, error) {
	r, body, errs := newPatch(serviceClient.config, requestPath).Send(serviceRequest).End()
	if errs != nil {
		return nil, fmt.Errorf("could not update service, error: %v", errs)
	}

	if r.StatusCode == 401 || r.StatusCode == 403 {
		return nil, fmt.Errorf("not authorised, message from kong: %s", body)
	}

	if r.StatusCode == 400 {
		return nil, fmt.Errorf("bad request, message from kong: %s", body)
	}

	updatedService := &Service{}
	err := json.Unmarshal([]byte(body), updatedService)
	if err != nil {
		return nil, fmt.Errorf("could not parse service update response, error: %v", err)
	}

	if updatedService.Id == nil {
		return nil, fmt.Errorf("could not update service, error: %v", body)
	}

	return updatedService, nil
}

func (serviceClient *serviceClient) CreatePluginConfig(serviceId string, pluginName string, pluginConfig string) (*ServicePluginConfig, error) {
	r, body, errs := newPost(serviceClient.config, ServicesPath+serviceId+"/"+pluginName).Send(pluginConfig).End()
	if errs != nil {
		return nil, fmt.Errorf("could not configure plugin for service, error: %v", errs)
	}

	if r.StatusCode == 401 || r.StatusCode == 403 {
		return nil, fmt.Errorf("not authorised, message from kong: %s", body)
	}

	createdServicePluginConfig := &ServicePluginConfig{}
	err := json.Unmarshal([]byte(body), createdServicePluginConfig)
	if err != nil {
		return nil, fmt.Errorf("could not parse service plugin config created response, error: %v", err)
	}

	if createdServicePluginConfig.Id == "" {
		return nil, fmt.Errorf("could not create service plugin config, error: %v", body)
	}

	createdServicePluginConfig.Body = body

	return createdServicePluginConfig, nil
}

func (serviceClient *serviceClient) GetPluginConfig(serviceId string, pluginName string, id string) (*ServicePluginConfig, error) {
	r, body, errs := newGet(serviceClient.config, ServicesPath+serviceId+"/"+pluginName+"/"+id).End()
	if errs != nil {
		return nil, fmt.Errorf("could not get plugin config for service, error: %v", errs)
	}

	if r.StatusCode == 401 || r.StatusCode == 403 {
		return nil, fmt.Errorf("not authorised, message from kong: %s", body)
	}

	servicePluginConfig := &ServicePluginConfig{}
	err := json.Unmarshal([]byte(body), servicePluginConfig)
	if err != nil {
		return nil, fmt.Errorf("could not parse service plugin config response, error: %v", err)
	}

	if servicePluginConfig.Id == "" {
		return nil, nil
	}

	servicePluginConfig.Body = body

	return servicePluginConfig, nil
}

func (serviceClient *serviceClient) GetPluginConfigs(serviceId string, pluginName string) ([]map[string]interface{}, error) {
	r, body, errs := newGet(serviceClient.config, ServicesPath+serviceId+"/"+pluginName).End()
	if errs != nil {
		return nil, fmt.Errorf("could not get plugin config for service, error: %v", errs)
	}

	if r.StatusCode == 401 || r.StatusCode == 403 {
		return nil, fmt.Errorf("not authorised, message from kong: %s", body)
	}

	servicePluginConfigs := &ServicePluginConfigs{}
	err := json.Unmarshal([]byte(body), servicePluginConfigs)
	if err != nil {
		return nil, fmt.Errorf("could not parse service plugin config response, error: %v", err)
	}
	if len(servicePluginConfigs.Data) == 0 {
		return nil, nil
	}

	return servicePluginConfigs.Data, nil
}

func (serviceClient *serviceClient) DeletePluginConfig(serviceId string, pluginName string, id string) error {
	r, body, errs := newDelete(serviceClient.config, ServicesPath+serviceId+"/"+pluginName+"/"+id).End()
	if errs != nil {
		return fmt.Errorf("could not delete plugin config for service, error: %v", errs)
	}

	if r.StatusCode == 401 || r.StatusCode == 403 {
		return fmt.Errorf("not authorised, message from kong: %s", body)
	}

	return nil
}

<?php

namespace League\Flysystem\Rackspace;

use GuzzleHttp\Exception\ClientException;
use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Adapter\Polyfill\NotSupportingVisibilityTrait;
use League\Flysystem\Adapter\Polyfill\StreamedCopyTrait;
use League\Flysystem\Config;
use League\Flysystem\FileNotFoundException;
use League\Flysystem\Util;
use GuzzleHttp\Client;

class RackspaceAdapter extends AbstractAdapter
{
    use StreamedCopyTrait;
    use NotSupportingVisibilityTrait;

    const US_IDENTITY_ENDPOINT = 'https://identity.api.rackspacecloud.com/v2.0/';
    const UK_IDENTITY_ENDPOINT = 'https://lon.identity.api.rackspacecloud.com/v2.0/';

    /**
     * @var string # Token access for the Cloud Files service
     */
    protected $TOKEN = null;

    /**
     * @var string # ENDPOINT for the Cloud Files service
     */
    protected $ENDPOINT = null;

    /**
     * @var string # CDN_ENDPOINT also from the cloud files service
     */
    protected $CDN_ENDPOINT = null;

    /**
     * @var string # To upload objects into a container
     */
    protected $containerName = null;

    /**
     * @var string # serviceCatalog of client
     */
    protected $serviceCatalogs = null;

    public function __construct($username, $apiKey, $IDENTITY_ENDPOINT)
    {
        $headers = ['Content-Type' => 'application/json'];
        $body = json_encode([
            "auth" => [
                "RAX-KSKEY:apiKeyCredentials" => [
                    "username" => $username,
                    "apiKey"   => $apiKey
                ]
            ]
        ]);

        $response = $this->request('POST',$IDENTITY_ENDPOINT.'tokens',$headers,$body);

        if (is_object($response)){
            $responseBody = json_decode($response->getBody());
            $this->TOKEN = $responseBody->access->token->id;
            $this->serviceCatalogs = $responseBody->access->serviceCatalog;
        }else{
            throw new \Exception($response);
        }
    }

    /**
     * @param string $containerName
     */
    public function setContainerName($containerName)
    {
        $this->containerName = $containerName;

        return $this;
    }

    /**
     * @param string $name    The name of the service as it appears in the Catalog
     * @param string $region  The region (DFW, IAD, ORD, LON, SYD)
     */
    public function objectStoreService($StoreServiceName, $StoreServiceRegion)
    {
        foreach ($this->serviceCatalogs AS $serviceCatalog){
            if ($serviceCatalog->name == $StoreServiceName){
                foreach ($serviceCatalog->endpoints AS $endpoint){
                    if ($endpoint->region == $StoreServiceRegion){
                        $this->ENDPOINT = $endpoint->publicURL;
                    }
                }
            }elseif ( $serviceCatalog->name == 'cloudFilesCDN' ){
                foreach ($serviceCatalog->endpoints AS $endpoint){
                    if ($endpoint->region == $StoreServiceRegion){
                        $this->CDN_ENDPOINT = $endpoint->publicURL;
                    }
                }
            }
        }
        return $this;
    }

    public function write($path, $contents, Config $config)
    {
        $headers = [];
        $headers['X-Auth-Token'] = $this->TOKEN;
        if ( $contents !== ''){
            $headers['Content-Type'] = mime_content_type($contents);
        }
        if ($config && $config->has('headers')) {
            $headers =  $config->get('headers');
        }
        $body    = $contents;
        $pathPrefix = $this->applyPathPrefix($this->containerName.'/'.$path);
        $response = $this->request('PUT', $this->ENDPOINT.'/'.$pathPrefix.'?'.'format=json', $headers, $body);

        if (is_object($response)){
            return $this->normalizeObject($this->headersNormalizeObject($response->getHeaders()), $pathPrefix);
        }else{
            return false;
        }
    }

    public function update($path, $contents, Config $config)
    {
        $headers = [];
        if ($config && $config->has('headers')) {
            $headers =  $config->get('headers');
        }
        $headers['X-Auth-Token'] = $this->TOKEN;
        $headers['Content-Type'] = mime_content_type($contents);
        $headers['X-Object-Meta-Some-Key'] = 'some-value';

        $body    = $contents;

        $pathPrefix = $this->applyPathPrefix($this->containerName.'/'.$path);

        $response = $this->request('PUT',$this->ENDPOINT.'/'.$pathPrefix, $headers, $body);

        if (is_object($response)){
            return $this->normalizeObject($this->headersNormalizeObject($response->getHeaders()), $pathPrefix);
        }else{
            return false;
        }
    }

    public function updateStream($path, $resource, Config $config)
    {
        return $this->update($path, $resource, $config);
    }

    public function rename($path, $newpath)
    {
        $relocation = $this->applyPathPrefix($this->containerName.'/'.$newpath);
        $pathPrefix = $this->applyPathPrefix($this->containerName.'/'.$path);

        $headers = ['X-Auth-Token' => $this->TOKEN, 'Destination' => $relocation];

        $response = $this->request('COPY',$this->ENDPOINT.'/'.$pathPrefix, $headers);

        if (is_object($response) && $response->getStatusCode() !== 201){
            return false;
        }

        $this->delete($path);

        return true;
    }

    public function delete($path)
    {
        $pathPrefix = $this->applyPathPrefix($this->containerName.'/'.$path);
        $headers = ['X-Auth-Token' => $this->TOKEN];

        $response = $this->request('DELETE',$this->ENDPOINT.'/'.$pathPrefix, $headers);

        if (is_object($response)){
            return true;
        }else{
            return false;
        }
    }

    public function deleteDir($dirname)
    {
        $response = $this->delete($dirname);

        if ( isset( $response->status ) && $response->status === 200) {
            return true;
        }

        return false;
    }

    public function createDir($dirname, Config $config)
    {
        $headers = $config->get('headers', []);
        $headers['X-Auth-Token'] = $this->TOKEN;
        $headers['Content-Type'] = 'application/directory';
        $extendedConfig = (new Config())->setFallback($config);
        $extendedConfig->set('headers', $headers);

        return $this->write($dirname, '', $extendedConfig);
    }

    public function has($path)
    {
        try {
            $location = $this->applyPathPrefix($path);
            $exists = $this->objectExists($location);
        } catch (\Exception $e) {
            return false;
        }

        return $exists;
    }

    public function read($path)
    {
        try {
            $headers = ['X-Auth-Token' => $this->TOKEN];
            $pathPrefix = $this->applyPathPrefix($this->containerName.'/'.$path);
            $response = $this->request('GET', $this->ENDPOINT.'/'.$pathPrefix, $headers);
            $data['contents'] = (string) $response->getBody();
            return $data;
        }catch (FileNotFoundException $exception){
            return false;
        }
    }

    public function listContents($directory = '', $recursive = false)
    {
        $response = [];
        $marker = null;
        $location = $this->applyPathPrefix($this->containerName.'/'.$directory);
        $headers['X-Auth-Token'] = $this->TOKEN;
        $request_response = $this->request('GET', $this->ENDPOINT.'/'.$location.'?'.'format=json', $headers);

        $objectsList = json_decode($request_response->getBody()->getContents(), true);
        if (count($objectsList) > 0){
            $objects = [];
            foreach ($objectsList AS $object){
                array_push($objects, $this->normalizeObject($object, $location));
            }
            $response = array_merge($response, $objects);
        }

        return Util::emulateDirectories($response);

    }

    public function getMetadata($path)
    {
        $pathPrefix = $this->applyPathPrefix($this->containerName.'/'.$path);

        $headers = ['X-Auth-Token' => $this->TOKEN];

        $response = $this->request('GET',$this->ENDPOINT.'/'.$pathPrefix, $headers);

        return $this->normalizeObject($this->headersNormalizeObject($response->getHeaders()), $pathPrefix);
    }

    public function getSize($path)
    {
        return $this->getMetadata($path);
    }

    public function getMimetype($path)
    {
        return $this->getMetadata($path);
    }

    public function getTimestamp($path)
    {
        return $this->getMetadata($path);
    }

    public function readStream($path)
    {
        return $this->read($path);
    }

    public function writeStream($path, $resource, Config $config)
    {
        return $this->write($path, $resource, $config);
    }

    public function enableContainer($path)
    {
        $headers = [];
        $headers['X-Auth-Token'] = $this->TOKEN;
        $headers['X-CDN-Enabled'] = 'True';
        $headers['X-TTL'] = 604800;
        $response = $this->request('PUT', $this->CDN_ENDPOINT.'/'.$this->containerName, $headers);

        return $response;
    }

    public function getPublicUrlCDN($path)
    {
        $headers = [];
        $headers['X-Auth-Token'] = $this->TOKEN;
        $pathPrefix = $this->applyPathPrefix($path);
        $response = $this->request('HEAD', $this->CDN_ENDPOINT.'/'.$this->containerName, $headers);

        $cdn_uri = $response->getHeaders()['X-Cdn-Uri'][0];

        return $cdn_uri.'/'.$pathPrefix;
    }

    public function applyPathPrefix($path)
    {
        $encodedPath = join('/', array_map('rawurlencode', explode('/', $path)));

        return parent::applyPathPrefix($encodedPath);
    }

    protected function request( $method, $url, array $headers = [], $body = null)
    {
        try {
            $client = new Client();

            $options = [];

            if (isset($headers) && !empty($headers)){
                $options['headers'] = $headers;
            }

            if (isset($body) && !empty($body)){
                $options['body'] = $body;
            }

            return $client->request($method, $url, $options);
        }catch (\Exception $exception){
            return json_encode([
                'message' =>$exception->getMessage()
            ]);
        }
    }

    protected function objectExists($name)
    {
        // Send HEAD request to check resource existence
        $headers = ['X-Auth-Token' => $this->TOKEN];
        $pathPrefix = $this->applyPathPrefix($this->containerName.'/'.$name);
        $response = $this->request('GET', $this->ENDPOINT.'/'.$pathPrefix, $headers);

        if (is_object($response)){
            return true;
        }else{
            return false;
        }
    }

    protected function objectList(array $params = array())
    {
        $params['format'] = 'json';

    }

    protected function headersNormalizeObject(array $object)
    {
        return [
            'content_type'    => $object['Content-Type'][0],
            'last_modified'   => $object['Last-Modified'][0],
            'bytes'  => $object['Content-Length'][0]
        ];
    }

    protected function normalizeObject(array $object ,$path)
    {
        if (isset($object['name'])){
            $name = $object['name'];
            $path = $path.$this->applyPathPrefix($name);
        }else{
            $name = $path;
        }
        $name = $this->removePathPrefix($name);
        $mimetype = explode('; ', $object['content_type']);

        return [
            'type'      => ((in_array('application/directory', $mimetype )) ? 'dir' : 'file'),
            'dirname'   => Util::dirname($name),
            'path'      => $path,
            'timestamp' => strtotime($object['last_modified']),
            'mimetype'  => reset($mimetype),
            'size'      => $object['bytes'],
        ];
    }

}
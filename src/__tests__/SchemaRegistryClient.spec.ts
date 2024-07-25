import { SchemaRegistryClient} from '../SchemaRegistryClient/SchemaRegistryClient';
import { RestService } from '../SchemaRegistryClient/RestService';
import { AxiosResponse } from 'axios';

jest.mock('../SchemaRegistryClient/RestService');

const baseUrls = ['http://mocked-url'];

let client: SchemaRegistryClient;
let restService: jest.Mocked<RestService>;

describe('SchemaRegistryClient-Register', () => { 
  
  beforeEach(() => {
    // Clear all instances and calls to constructor and all methods:
    restService = new RestService(baseUrls) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(restService);
  });
  
  afterEach(() => {
    jest.clearAllMocks();
  });
  
  it('Should return id when Register is called', async () => {
    const mockSubject = 'mock-subject';
    const schemaString = JSON.stringify({
      type: 'record',
      name: 'User',
      fields: [
        { name: 'name', type: 'string' },
        { name: 'age', type: 'int' }
      ]
    });

    const schemaInfo = {
        schema: schemaString,
        schemaType: 'AVRO',
    };

    restService.sendHttpRequest.mockResolvedValue({ data: { id: 1 } } as AxiosResponse);
  
    const result = await client.register(mockSubject, schemaInfo);
  
    expect(result).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return from cache when Register is called twice', async () => {
    const mockSubject = 'mock-subject';
    const schemaString = JSON.stringify({
      type: 'record',
      name: 'User',
      fields: [
        { name: 'name', type: 'string' },
        { name: 'age', type: 'int' }
      ]
    });

    const schemaInfo = {
        schema: schemaString,
        schemaType: 'AVRO',
    };

    restService.sendHttpRequest.mockResolvedValue({ data: { id: 1 } } as AxiosResponse);
  
    const result = await client.register(mockSubject, schemaInfo);
    expect(result).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
    
    const result2 = await client.register(mockSubject, schemaInfo);
    expect(result2).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should throw error when Register is called with invalid schema', async () => {
    const mockSubject = 'mock-subject';
    const schemaString = JSON.stringify({
      type: 'record',
      name: 'User',
      fields: [
        { name: 'name', type: 'string' },
        { name: 'age', type: 'int' }
      ]
    });
    
    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
    };
    
    restService.sendHttpRequest.mockRejectedValue(new Error('Invalid schema'));
    
    await expect(client.register(mockSubject, schemaInfo)).rejects.toThrow('Invalid schema');
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return id, version, metadata, and schema when RegisterFullResponse is called', async () => {
    const mockSubject = 'mock-subject';
    const schemaString = JSON.stringify({
      type: 'record',
      name: 'User',
      fields: [
        { name: 'name', type: 'string' },
        { name: 'age', type: 'int' }
      ]
    });
    
    const metadata = {
      properties: {
      owner: 'Alice Bob',
      email: 'Alice@bob.com',
      }
    };
    
    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
      metadata: metadata,
    };
    
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    
    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);
    
    const result = await client.registerFullResponse(mockSubject, schemaInfo);
    
    expect(result).toEqual(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return id, version, metadata, and schema from cache when RegisterFullResponse is called twice', async () => {
    const mockSubject = 'mock-subject';
    const schemaString = JSON.stringify({
      type: 'record',
      name: 'User',
      fields: [
        { name: 'name', type: 'string' },
        { name: 'age', type: 'int' }
      ]
    });
    
    const metadata = {
      properties: {
        owner: 'Alice Bob',
        email: 'Alice@bob.com',
      }
    };
    
    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
      metadata: metadata,
    };
    
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    
    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);
    
    const result = await client.registerFullResponse(mockSubject, schemaInfo);
    expect(result).toEqual(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);

    const result2 = await client.registerFullResponse(mockSubject, schemaInfo);
    expect(result2).toEqual(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it ('Should throw error when RegisterFullResponse is called with invalid schema', async () => {
    const mockSubject = 'mock-subject';
    const schemaString = JSON.stringify({
      type: 'record',
      name: 'User',
      fields: [
        { name: 'name', type: 'string' },
        { name: 'age', type: 'int' }
      ]
    });
    
    const metadata = {
      properties: {
        owner: 'Alice Bob',
        email: 'alice@bob.com',
      }
    };

    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
      metadata: metadata,
    };

    restService.sendHttpRequest.mockRejectedValue(new Error('Invalid schema'));

    await expect(client.registerFullResponse(mockSubject, schemaInfo)).rejects.toThrow('Invalid schema');
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });
});

describe('SchemaRegistryClient-Get-ID', () => {
  beforeEach(() => {
    restService = new RestService(baseUrls) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(restService);
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should return id when GetId is called', async () => {
    const mockSubject = 'mock-subject';
    const schemaString = JSON.stringify({
      type: 'record',
      name: 'User',
      fields: [
        { name: 'name', type: 'string' },
        { name: 'age', type: 'int' }
      ]
    });

    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
    };

    restService.sendHttpRequest.mockResolvedValue({ data: { id: 1 } } as AxiosResponse);
  
    const result = await client.getId(mockSubject, schemaInfo);
  
    expect(result).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });
});

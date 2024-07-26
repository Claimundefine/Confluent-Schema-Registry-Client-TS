import { SchemaRegistryClient, Metadata, Compatibility } from '../SchemaRegistryClient/SchemaRegistryClient';
import { RestService } from '../SchemaRegistryClient/RestService';
import { AxiosResponse } from 'axios';

jest.mock('../SchemaRegistryClient/RestService');

const baseUrls = ['http://mocked-url'];

let client: SchemaRegistryClient;
let restService: jest.Mocked<RestService>;
const mockSubject = 'mock-subject';
const mockSubject2 = 'mock-subject2';
const schemaString = JSON.stringify({
  type: 'record',
  name: 'User',
  fields: [
    { name: 'name', type: 'string' },
    { name: 'age', type: 'int' }
  ]
});
const schemaString2 = JSON.stringify({
  type: 'record',
  name: 'User2',
  fields: [
    { name: 'name2', type: 'string' },
    { name: 'age2', type: 'int' }
  ]
});
const metadata: Metadata = {
  properties: {
  owner: 'Alice Bob',
  email: 'Alice@bob.com',
  }
};
const metadata2: Metadata = {
  properties: {
  owner: 'Alice Bob2',
  email: 'Alice@bob2.com',
  }
};
const subjects: string[] = [mockSubject, mockSubject2];
const versions: number[] = [1, 2, 3];
const compatibility: Compatibility = 1;

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
    const schemaInfo = {
        schema: schemaString,
        schemaType: 'AVRO',
    };

    const schemaInfo2 = {
        schema: schemaString2,
        schemaType: 'AVRO',
    };

    restService.sendHttpRequest.mockResolvedValue({ data: { id: 1 } } as AxiosResponse);
  
    const result = await client.register(mockSubject, schemaInfo);
    expect(result).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);

    restService.sendHttpRequest.mockResolvedValue({ data: { id: 2 } } as AxiosResponse);
    
    const result2 = await client.register(mockSubject2, schemaInfo2);
    expect(result2).toEqual(2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    //Try to create same objects again

    const cachedResult = await client.register(mockSubject, schemaInfo);
    expect(cachedResult).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult2 = await client.register(mockSubject2, schemaInfo2);
    expect(cachedResult2).toEqual(2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);
  });

  it('Should return id, version, metadata, and schema when RegisterFullResponse is called', async () => {
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
    
    expect(result).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return id, version, metadata, and schema from cache when RegisterFullResponse is called twice', async () => {
    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
      metadata: metadata,
    };
    const schemaInfo2 = {
      schema: schemaString2,
      schemaType: 'AVRO',
      metadata: metadata2,
    };
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    const response2 = {
      id: 2,
      version: 1,
      schema: schemaString2,
      metadata: metadata2,
    };
    
    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);
    
    const result = await client.registerFullResponse(mockSubject, schemaInfo);
    expect(result).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);

    restService.sendHttpRequest.mockResolvedValue({ data: response2 } as AxiosResponse);

    const result2 = await client.registerFullResponse(mockSubject2, schemaInfo2);
    expect(result2).toMatchObject(response2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult = await client.registerFullResponse(mockSubject, schemaInfo);
    expect(cachedResult).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult2 = await client.registerFullResponse(mockSubject2, schemaInfo2);
    expect(cachedResult2).toMatchObject(response2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);
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
    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
    };

    restService.sendHttpRequest.mockResolvedValue({ data: { id: 1 } } as AxiosResponse);
  
    const result = await client.getId(mockSubject, schemaInfo);
  
    expect(result).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return id from cache when GetId is called twice', async () => {
    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
    };
    const schemaInfo2 = {
      schema: schemaString2,
      schemaType: 'AVRO',
    };

    restService.sendHttpRequest.mockResolvedValue({ data: { id: 1 } } as AxiosResponse);
  
    const result = await client.getId(mockSubject, schemaInfo);
    expect(result).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);

    restService.sendHttpRequest.mockResolvedValue({ data: { id: 2 } } as AxiosResponse);

    const result2 = await client.getId(mockSubject2, schemaInfo2);
    expect(result2).toEqual(2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult = await client.getId(mockSubject, schemaInfo);
    expect(cachedResult).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult2 = await client.getId(mockSubject2, schemaInfo2);
    expect(cachedResult2).toEqual(2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);
  });

  it('Should return SchemaInfo when GetBySubjectAndId is called', async () => {
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };

    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);
    
    const result = await client.getBySubjectAndId(mockSubject, 1);
    
    expect(result).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return SchemaInfo from cache when GetBySubjectAndId is called twice', async () => {
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    const response2 = {
      id: 2,
      version: 1,
      schema: schemaString2,
      metadata: metadata2,
    };

    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);

    const result = await client.getBySubjectAndId(mockSubject, 1);
    expect(result).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);

    restService.sendHttpRequest.mockResolvedValue({ data: response2 } as AxiosResponse);

    const result2 = await client.getBySubjectAndId(mockSubject2, 2);
    expect(result2).toMatchObject(response2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult = await client.getBySubjectAndId(mockSubject, 1);
    expect(cachedResult).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult2 = await client.getBySubjectAndId(mockSubject2, 2);
    expect(cachedResult2).toMatchObject(response2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);
  });
});

describe('SchemaRegistryClient-Get-Schema-Metadata', () => {
  beforeEach(() => {
    restService = new RestService(baseUrls) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(restService);
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should return latest schema with metadata when GetLatestWithMetadata is called', async () => {
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };

    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);
    
    const result = await client.getLatestWithMetadata(mockSubject, metadata);
    
    expect(result).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return latest schema with metadata from cache when GetLatestWithMetadata is called twice', async () => {
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    const response2 = {
      id: 2,
      version: 1,
      schema: schemaString2,
      metadata: metadata2,
    };

    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);

    const result = await client.getLatestWithMetadata(mockSubject, metadata);
    expect(result).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);

    restService.sendHttpRequest.mockResolvedValue({ data: response2 } as AxiosResponse);

    const result2 = await client.getLatestWithMetadata(mockSubject2, metadata2);
    expect(result2).toMatchObject(response2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult = await client.getLatestWithMetadata(mockSubject, metadata);
    expect(cachedResult).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult2 = await client.getLatestWithMetadata(mockSubject2, metadata2);
    expect(cachedResult2).toMatchObject(response2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);
  });

  it('Should return SchemaMetadata when GetSchemaMetadata is called', async () => {
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };

    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);
    
    const result = await client.getSchemaMetadata(mockSubject, 1, true);
    
    expect(result).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return SchemaMetadata from cache when GetSchemaMetadata is called twice', async () => {
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    const response2 = {
      id: 2,
      version: 1,
      schema: schemaString2,
      metadata: metadata2,
    };

    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);

    const result = await client.getSchemaMetadata(mockSubject, 1, true);
    expect(result).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);

    restService.sendHttpRequest.mockResolvedValue({ data: response2 } as AxiosResponse);

    const result2 = await client.getSchemaMetadata(mockSubject2, 2, false);
    expect(result2).toMatchObject(response2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult = await client.getSchemaMetadata(mockSubject, 1, true);
    expect(cachedResult).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult2 = await client.getSchemaMetadata(mockSubject2, 2, false);
    expect(cachedResult2).toMatchObject(response2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);
  });

  it ('Should get latest schema with metadata when GetLatestWithMetadata is called', async () => {
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };

    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);

    const result = await client.getLatestWithMetadata(mockSubject, metadata);

    expect(result).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it ('Should get latest schema with metadata from cache when GetLatestWithMetadata is called twice', async () => {
    const response = {
      id: 1,
      version: 1,
      schema: schemaString,
      metadata: metadata,
    };
    const response2 = {
      id: 2,
      version: 1,
      schema: schemaString2,
      metadata: metadata2,
    };

    restService.sendHttpRequest.mockResolvedValue({ data: response } as AxiosResponse);

    const result = await client.getLatestWithMetadata(mockSubject, metadata);
    expect(result).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);

    restService.sendHttpRequest.mockResolvedValue({ data: response2 } as AxiosResponse);

    const result2 = await client.getLatestWithMetadata(mockSubject2, metadata2);
    expect(result2).toMatchObject(response2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult = await client.getLatestWithMetadata(mockSubject, metadata);
    expect(cachedResult).toMatchObject(response);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult2 = await client.getLatestWithMetadata(mockSubject2, metadata2);
    expect(cachedResult2).toMatchObject(response2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);
  });
});

describe('SchemaRegistryClient-Subjects', () => {
  beforeEach(() => {
    restService = new RestService(baseUrls) as jest.Mocked<RestService>;
    client = new SchemaRegistryClient(restService);
  });
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('Should return all subjects when GetAllSubjects is called', async () => {
    restService.sendHttpRequest.mockResolvedValue({ data: subjects } as AxiosResponse);
    
    const result = await client.getAllSubjects();
    
    expect(result).toEqual(subjects);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return all versions when GetAllVersions is called', async () => {
    restService.sendHttpRequest.mockResolvedValue({ data: versions } as AxiosResponse);
    
    const result = await client.getAllVersions(mockSubject);
    
    expect(result).toEqual(versions);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return version when GetVersion is called', async () => {
    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
    };
    restService.sendHttpRequest.mockResolvedValue({ data: {version: 1} } as AxiosResponse);
    
    const result = await client.getVersion(mockSubject, schemaInfo, true);
    
    expect(result).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);
  });

  it('Should return version from cache when GetVersion is called twice', async () => {
    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
    };
    const schemaInfo2 = {
      schema: schemaString2,
      schemaType: 'AVRO',
    };

    restService.sendHttpRequest.mockResolvedValue({ data: {version: 1} } as AxiosResponse);
    
    const result = await client.getVersion(mockSubject, schemaInfo, true);
    expect(result).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(1);

    restService.sendHttpRequest.mockResolvedValue({ data: {version: 2} } as AxiosResponse);

    const result2 = await client.getVersion(mockSubject2, schemaInfo2, false);
    expect(result2).toEqual(2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult = await client.getVersion(mockSubject, schemaInfo, true);
    expect(cachedResult).toEqual(1);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);

    const cachedResult2 = await client.getVersion(mockSubject2, schemaInfo2, false);
    expect(cachedResult2).toEqual(2);
    expect(restService.sendHttpRequest).toHaveBeenCalledTimes(2);
  });

  //TODO: Add test for delete subject and delete subject version
});

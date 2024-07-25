import { RestService } from './RestService';
import { AxiosResponse } from 'axios';
import { LRUCache } from 'lru-cache';
import { Mutex } from 'async-mutex';

/*
 * Confluent-Schema-Registry-TypeScript - Node.js wrapper for Confluent Schema Registry
 *
 * Copyright (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

enum Compatibility {
  None = 1,
  Backward = 2,
  Foward = 3,
  Full = 4,
  BackwardTransitive = 5,
  ForwardTransitive = 6,
  FullTransitive = 7
}

interface Rule {
  name: string;
  subject: string;
  version: number;
}

interface SchemaInfo {
  schema?: string;
  schemaType?: string;
  references?: Reference[];
  metadata?: Metadata;
  ruleSet?: RuleSet;
}

interface SchemaMetadata extends SchemaInfo {
  id: number;
  subject?: string;
  version?: number;
}

interface Reference {
  Name: string;
  Subject: string;
  Version: number;
}

interface Metadata {
  tags?: { [key: string]: string[] };
  properties?: { [key: string]: string };
  sensitive?: string[];
}

interface RuleSet {
  migrationRules: Rule[];
  compatibilityRules: Rule[];
}

interface ServerConfig {
  Alias: string;
  Normalize: boolean;
  CompatibilityUpdate: Compatibility;
  CompatibilityLevel: Compatibility;
  CompatibilityGroup: string;
  DefaultMetadata: Metadata;
  OverrideMetadata: Metadata;
  DefaultRuleSet: RuleSet;
  OverrideRuleSet: RuleSet;
}

class SchemaRegistryClient {
  private restService: RestService;
  private schemaToIdCache: LRUCache<string, number>;
  private idToSchemaInfoCache: LRUCache<string, SchemaInfo>;
  private infoToSchemaCache: LRUCache<string, SchemaMetadata>;
  private latestToSchemaCache: LRUCache<string, SchemaMetadata>;
  private schemaToVersionCache: LRUCache<string, number>;
  private versionToSchemaCache: LRUCache<string, SchemaMetadata>;
  private metadataToSchemaCache: LRUCache<string, SchemaMetadata>;
  private missingSchemaCache: LRUCache<number, boolean>;
  private missingIdCache: LRUCache<number, boolean>;
  private missingVersionCache: LRUCache<string, boolean>;
  private schemaToResponseMutex: Mutex;
  private schemaToIdMutex: Mutex;
  private idToSchemaInfoMutex: Mutex;
  private infoToSchemaMutex: Mutex;
  private latestToSchemaMutex: Mutex;
  private schemaToVersionMutex: Mutex;
  private versionToSchemaMutex: Mutex;
  private metadataToSchemaMutex: Mutex;
  private missingSchemaMutex: Mutex;
  private missingIdMutex: Mutex;
  private missingVersionMutex: Mutex;

  constructor(restService: RestService, cacheSize: number = 512, cacheTTL?: number) {
    const cacheOptions = {
       max: cacheSize,
       ...(cacheTTL !== undefined && { maxAge: cacheTTL }),
       ...(cacheTTL !== undefined && { stale: false }),
    }; 
    // this.schemaToResponseCache = new LRUCache(cacheOptions);
    this.restService = restService;
    this.schemaToIdCache = new LRUCache(cacheOptions);
    this.idToSchemaInfoCache = new LRUCache(cacheOptions);
    this.infoToSchemaCache = new LRUCache(cacheOptions);
    this.latestToSchemaCache = new LRUCache(cacheOptions);
    this.schemaToVersionCache = new LRUCache(cacheOptions);
    this.versionToSchemaCache = new LRUCache(cacheOptions);
    this.metadataToSchemaCache = new LRUCache(cacheOptions);
    this.missingSchemaCache = new LRUCache(cacheOptions);
    this.missingIdCache = new LRUCache(cacheOptions);
    this.missingVersionCache = new LRUCache(cacheOptions);
    this.schemaToResponseMutex = new Mutex();
    this.schemaToIdMutex = new Mutex();
    this.idToSchemaInfoMutex = new Mutex();
    this.infoToSchemaMutex = new Mutex();
    this.latestToSchemaMutex = new Mutex();
    this.schemaToVersionMutex = new Mutex();
    this.versionToSchemaMutex = new Mutex();
    this.metadataToSchemaMutex = new Mutex();
    this.missingSchemaMutex = new Mutex();
    this.missingIdMutex = new Mutex();
    this.missingVersionMutex = new Mutex();
  }

  public async register(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<number> {
     const metadata: SchemaMetadata = await this.registerFullResponse(subject, schema, normalize);

     return metadata.id;
  }

  public async registerFullResponse(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<SchemaMetadata> {
    const cacheKey = JSON.stringify({ subject, schema });

    return await this.infoToSchemaMutex.runExclusive(async () => {
      const cachedSchemaMetadata: SchemaMetadata | undefined = this.infoToSchemaCache.get(cacheKey);
      if (cachedSchemaMetadata) {
        console.log("registerFullResponse2: Cache hit");
        return cachedSchemaMetadata;
      }

      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}/versions?normalize=${normalize}`,
        'POST',
        schema
      );

      this.infoToSchemaCache.set(cacheKey, response.data);
      
      return response.data;
    });
  }

  public async getBySubjectAndId(subject: string, id: number): Promise<SchemaInfo> {
    const cacheKey = JSON.stringify({ subject, id });
    return await this.idToSchemaInfoMutex.runExclusive(async () => {
      const cachedSchema: SchemaInfo | undefined = this.idToSchemaInfoCache.get(cacheKey);
      if (cachedSchema) {
        console.log("getBySubjectAndId: Cache hit");
        return cachedSchema;
      }

      const response: AxiosResponse<SchemaInfo> = await this.restService.sendHttpRequest(
        `/subjects/${subject}/versions/${id}`,
        'GET'
      );
      this.idToSchemaInfoCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  public async getId(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<number> {
    const cacheKey = JSON.stringify({ subject, schema });
    
    console.log("in get Id");
    return await this.schemaToIdMutex.runExclusive(async () => {
      const cachedId: number | undefined = this.schemaToIdCache.get(cacheKey);
      if (cachedId) {
        console.log("getId: Cache hit");
        return cachedId;
      }

      console.log("not returning cache get Id");
      
      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}?normalize=${normalize}`,
        'POST',
        schema
      );
      this.schemaToIdCache.set(cacheKey, response.data.id);
      return response.data.id;
    });
  }

  public async getLatestSchemaMetadata(subject: string): Promise<SchemaMetadata> {
    return await this.latestToSchemaMutex.runExclusive(async () => {
      const cachedSchema: SchemaMetadata | undefined = this.latestToSchemaCache.get(subject);
      if (cachedSchema) {
        console.log("getLatestSchemaMetadata: Cache hit");
        return cachedSchema;
      }

      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}/versions/latest`,
        'GET'
      );
      this.latestToSchemaCache.set(subject, response.data);
      return response.data;
    });
  }

  public async getSchemaMetadata(subject: string, version: number, deleted: boolean = false): Promise<SchemaMetadata> {
    const cacheKey = JSON.stringify({ subject, version, deleted });
  
    return await this.versionToSchemaMutex.runExclusive(async () => {
      const cachedSchemaMetadata: SchemaMetadata | undefined = this.versionToSchemaCache.get(JSON.stringify(cacheKey));
      if (cachedSchemaMetadata) {
        console.log("getSchemaMetadata: Cache hit");
        return cachedSchemaMetadata;
      }

      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}/versions/${version}?deleted=${deleted}`,
        'GET'
      );
      this.versionToSchemaCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  //TODO: Get clarification with getLatestWithMetadata
  public async getLatestWithMetadata(subject: string, metadata: Metadata, deleted: boolean = false): Promise<SchemaMetadata> {
    const cacheKey = JSON.stringify({ subject, metadata, deleted });

    return await this.metadataToSchemaMutex.runExclusive(async () => {
      const cachedSchemaMetadata: SchemaMetadata | undefined = this.metadataToSchemaCache.get(cacheKey);
      if (cachedSchemaMetadata) {
        console.log("getLatestWithMetadata: Cache hit");
        return cachedSchemaMetadata;
      }

      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}/metadata?deleted=${deleted}`,
        'GET',
        metadata
      );
      this.metadataToSchemaCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  public async getAllVersions(subject: string): Promise<number[]> {
    const response: AxiosResponse<number[]> = await this.restService.sendHttpRequest(
      `/subjects/${subject}/versions`,
      'GET'
    );
    return response.data;
  }

  public async getVersion(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<number> {

    const cacheKey = JSON.stringify({ subject, schema });
    await this.schemaToVersionMutex.runExclusive(async () => {
      const cachedVersion: number | undefined = this.schemaToVersionCache.get(cacheKey);
      if (cachedVersion) {
        console.log("getVersion: Cache hit");
        return cachedVersion;
      }
    });

    return await this.schemaToVersionMutex.runExclusive(async () => {
      const response: AxiosResponse<number> = await this.restService.sendHttpRequest(
        `/subjects/${subject}?normalize=${normalize}`,
        'POST',
        schema
      );
      this.schemaToVersionCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  public async getAllSubjects(): Promise<string[]> {
    const response: AxiosResponse<string[]> = await this.restService.sendHttpRequest(
      `/subjects`,
      'GET'
    );
    return response.data;
  }

  public async deleteSubject(subject: string, permanent: boolean = false): Promise<number[]> {
    await this.infoToSchemaMutex.runExclusive(async () => {
      this.infoToSchemaCache.forEach((_, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject) {
           this.infoToSchemaCache.delete(key);
        }
      });
    });

    await this.schemaToVersionMutex.runExclusive(async () => {
      this.schemaToVersionCache.forEach((_, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject) {
          this.schemaToVersionCache.delete(key);
        }
      });
    });

    await this.versionToSchemaMutex.runExclusive(async () => {
      this.versionToSchemaCache.forEach((_, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject) {
          this.versionToSchemaCache.delete(key);
        }
      });
    });

    await this.idToSchemaInfoMutex.runExclusive(async () => {
      this.idToSchemaInfoCache.forEach((_, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject) {
          this.idToSchemaInfoCache.delete(key);
        }
      });
    });

    //TODO: latestToSchemaCache, metadataToSchemaCache?

    const response: AxiosResponse<number[]> = await this.restService.sendHttpRequest(
      `/subjects/${subject}?permanent=${permanent}`,
      'DELETE'
    );
    return response.data;
  }

  public async deleteSubjectVersion(subject: string, version: number, permanent: boolean = false): Promise<number[]> {
    await this.schemaToVersionMutex.runExclusive(async () => {

      this.schemaToVersionCache.forEach(async (value, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject && value === version) {
          this.schemaToVersionCache.delete(key);
          await this.infoToSchemaMutex.runExclusive(async () => {
            const deleted = this.infoToSchemaCache.delete(key);
          });
        }
      });
    });

    const response: AxiosResponse<number[]> = await this.restService.sendHttpRequest(
      `/subjects/${subject}/versions/${version}?permanent=${permanent}`,
      'DELETE'
    );
    return response.data;
  }

  public async testSubjectCompatibility(subject: string, schema: SchemaInfo): Promise<boolean> {
    const response: AxiosResponse<boolean> = await this.restService.sendHttpRequest(
      `/compatibility/subjects/${subject}/versions/latest`,
      'POST',
      schema
    );
    return response.data;
  }

  public async testCompatibility(subject: string, version: number, schema: SchemaInfo): Promise<boolean> {
    const response: AxiosResponse<boolean> = await this.restService.sendHttpRequest(
      `/compatibility/subjects/${subject}/versions/${version}`,
      'POST',
      schema
    );
    return response.data;
  }

  public async getCompatibility(subject: string): Promise<Compatibility> {
    const response: AxiosResponse<Compatibility> = await this.restService.sendHttpRequest(
      `/config/${subject}/compatibility`,
      'GET'
    );

    return response.data;
  }

  public async updateCompatibility(subject: string, update: Compatibility): Promise<Compatibility> {
    const response: AxiosResponse<Compatibility> = await this.restService.sendHttpRequest(
      `/config/${subject}/compatibility`,
      'PUT',
      update
    );

    return response.data;
  }

  public async getDefaultCompatibility(): Promise<Compatibility> {
    const response: AxiosResponse<Compatibility> = await this.restService.sendHttpRequest(
      `/config`,
      'GET'
    );

    return response.data;
  }

  public async updateDefaultCompatibility(update: Compatibility): Promise<Compatibility> {
    const response: AxiosResponse<Compatibility> = await this.restService.sendHttpRequest(
      `/config`,
      'PUT',
      update
    );

    return response.data;
  }

  public async getConfig(subject: string): Promise<ServerConfig> {
    const response: AxiosResponse<ServerConfig> = await this.restService.sendHttpRequest(
      `/config/${subject}`,
      'GET'
    );

    return response.data;
  }

  public async updateConfig(subject: string, update: ServerConfig): Promise<ServerConfig> {
    const response: AxiosResponse<ServerConfig> = await this.restService.sendHttpRequest(
      `/config/${subject}`,
      'PUT',
      update
    );

    return response.data;
  }

  public async getGlobalConfig(): Promise<ServerConfig> {
    const response: AxiosResponse<ServerConfig> = await this.restService.sendHttpRequest(
      `/config`,
      'GET'
    );

    return response.data;
  }

  public async updateGlobalConfig(update: ServerConfig): Promise<ServerConfig> {
    const response: AxiosResponse<ServerConfig> = await this.restService.sendHttpRequest(
      `/config`,
      'PUT',
      update
    );

    return response.data;
  }

  public close(): void {
    return;
  }

}

export { SchemaRegistryClient, SchemaInfo };

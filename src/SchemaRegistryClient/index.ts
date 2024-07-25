const { RestService } = require('./RestService');
const { SchemaRegistryClient } = require('./SchemaRegistryClient');

// Configuration for the RestService
const baseUrls = ['http://localhost:8081']
const headers = { 'Content-Type': 'application/vnd.schemaregistry.v1+json' }
const restService = new RestService(baseUrls, false)
restService.setHeaders(headers)

// Optionally set up authenticationsrl
const basicAuth = Buffer.from('RBACAllowedUser-lsrc1:nohash').toString('base64')
restService.setAuth(basicAuth)

// Optionally set other configurations
restService.setTimeout(10000) // Set timeout to 10 seconds

// Create an instance of the SchemaRegistryClient
const schemaRegistryClient = new SchemaRegistryClient(restService)
const curSubject = 'test-subject8'

async function main() {
  try {
    // Example: Get all subjects
    const subjects = await schemaRegistryClient.getAllSubjects()
    console.log('All Subjects:', subjects)

    // Example: Register a new schema
    const schemaString = JSON.stringify({
      type: 'record',
      name: 'User',
      fields: [
        { name: 'name4', type: 'string' },
        { name: 'age', type: 'int' },
      ],
    })

    const metadata1 = {
      properties: {
        owner: 'Bob Jones',
        email: 'bob@acme.com',
      },
    }

    const schemaInfo = {
      schema: schemaString,
      schemaType: 'AVRO',
      metadata: metadata1,
    }

    const forwardCompatibleSchemaString = JSON.stringify({
      type: 'record',
      name: 'User',
      fields: [
        { name: 'name4', type: 'string' },
        { name: 'age', type: 'int' },
        { name: 'email', type: ['null', 'string'], default: null },
        { name: 'isActive', type: 'boolean', default: true },
        { name: 'createdDate', type: ['null', 'string'], default: null },
      ],
    })

    const metadata2 = {
      properties: {
        owner: 'Bob Jones2',
        email: 'bob@acme.com',
      },
    }

    const forwardCompatibleSchemaInfo = {
      schema: forwardCompatibleSchemaString,
      schemaType: 'AVRO',
      metadata: metadata2,
    }

    const id = await schemaRegistryClient.register(curSubject, schemaInfo);
    console.log('Registered Schema:', id);

    const id2 = await schemaRegistryClient.register(curSubject, schemaInfo);
    console.log('Registered Schema2:', id2);

    let id7 = await schemaRegistryClient.registerFullResponse(
      curSubject,
      forwardCompatibleSchemaInfo
    );
    console.log('Registered Schema7:', id7);

    let id72 = await schemaRegistryClient.register(curSubject, forwardCompatibleSchemaInfo);
    console.log('Registered Schema72:', id72);

    // Example: Get a schema by ID
    const id1 = await schemaRegistryClient.getId(curSubject, schemaInfo)
    console.log('Schema by ID:', id1)

    const id3 = await schemaRegistryClient.getId(curSubject, schemaInfo)
    console.log('Schema2 by ID:', id3)

    const schemaMetadataxd = await schemaRegistryClient.getLatestWithMetadata(curSubject, metadata1)
    console.log('Schema Metadata:', schemaMetadataxd)

    // // Example: Get the latest version of a schema for a subject
    // const latestSchema = await schemaRegistryClient.getLatestVersion(curSubject);
    // console.log('Latest Schema Version:', latestSchema);

    // Example: Delete a subject
    let curSubjectResponse = await schemaRegistryClient.deleteSubject(curSubject)
    console.log('Soft deleted subject:', curSubject, curSubjectResponse)
    curSubjectResponse = await schemaRegistryClient.deleteSubject(curSubject, true)
    console.log('Hard deleted subject:', curSubject, curSubjectResponse)
  } catch (error) {
    console.error('Error:', error)
  }
}

// Run the main function
main()

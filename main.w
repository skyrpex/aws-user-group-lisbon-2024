bring cloud;

interface Storage {
    inflight putObject(key: str, contents: str): void;
    inflight getObject(key: str): str;

    onObjectCreated(callback: inflight (str): void): void;
}

class BucketStorage impl Storage {
    bucket: cloud.Bucket;

    new() {
        this.bucket = new cloud.Bucket();
    }

    pub inflight putObject(key: str, contents: str) {
        this.bucket.put(key, contents);
    }

    pub inflight getObject(key: str): str {
        return this.bucket.get(key);
    }

    pub onObjectCreated(callback: inflight (str): void) {
        this.bucket.onCreate(inflight (key) => {
            callback(key);
        });
    }
}

bring dynamodb;

class TableStorage impl Storage {
    table: dynamodb.Table;
    topic: cloud.Topic;

    new() {
        this.table = new dynamodb.Table(
            hashKey: "key",
            attributes: [
                { name: "key", type: "S" },
            ],
        );

        this.topic = new cloud.Topic();

        this.table.setStreamConsumer(inflight (record) => {
            if let key = record.dynamodb.NewImage?.get("key")?.get("S")?.asStr() {
                this.topic.publish(key);
            }
        });
    }

    pub inflight putObject(key: str, contents: str) {
        this.table.put(
            Item: {
                key,
                contents,
            },
        );
    }

    pub inflight getObject(key: str): str {
        let item = this.table.get(
            Key: {
                key,
            },
        ).Item!;
        return item.get("contents").asStr();
    }

    pub onObjectCreated(callback: inflight (str): void) {
        this.topic.onMessage(inflight (key) => {
            callback(key);
        });
    }
}

class FileProcessor {
    uploads: Storage;
    processedObjects: Storage;

    new(uploads: Storage, processed: Storage) {
        this.uploads = uploads;
        this.processedObjects = processed;

        this.uploads.onObjectCreated(inflight (key) => {
            let contents = this.uploads.getObject(key);
            let uppercaseContents = contents.uppercase();
            this.processedObjects.putObject(key, uppercaseContents);
        });
    }

    pub inflight processObject(key: str, contents: str) {
        this.uploads.putObject(key, contents);
    }

    pub inflight getProcessedObject(key: str): str {
        return this.processedObjects.getObject(key);
    }

    pub onFileProcessed(callback: inflight (str): void) {
        this.processedObjects.onObjectCreated(inflight (key) => {
            callback(key);
        });
    }
}

let processor = new FileProcessor(
    new BucketStorage() as "UploadsStorage",
    new TableStorage() as "ProcessedStorage",
);

processor.onFileProcessed(inflight (key) => {
    let fileContents = processor.getProcessedObject(key);
    log("Object [{key}] was processed: {fileContents}");
});

bring util;

new cloud.Function(inflight () => {
    let key = "hello world {util.nanoid()}";
    processor.processObject(key, key);
}) as "ProcessRandomObject";

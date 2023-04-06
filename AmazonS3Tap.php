<?php

use GuzzleHttp\Client;
use SingerPhp\SingerTap;
use SingerPhp\Singer;

use Aws\S3\S3Client;
use Aws\Exception\AwsException;

class AmazonS3Tap extends SingerTap
{
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Maximum number of API retries
     * @var integer
     */
    const MAX_RETRIES = 5;

    /**
     * Retry interval
     * @var integer
     */
    const RETRY_INTERVAL = 30;

    /**
     * Root directory path of schemas
     * @var string
     */
    const SCHEMAS_ROOT = 'clients/data-lake/schemas/';

    /**
     * Root directory path of data
     * @var string
     */
    const DATA_ROOT = 'clients/data-lake/events/';

    /**
     * Access Key ID
     * @var object
     */
    private $client = null;

    /**
     * Access Key ID
     * @var string
     */
    private $access_key = '';

    /**
     * Secret Access Key
     * @var string
     */
    private $secret_key = '';

    /**
     * AWS S3 Bucket Name
     * @var string
     */
    private $bucket_name = '';

    /**
     * AWS S3 Bucket Region
     * @var string
     */
    private $region = '';

    /**
     * tests if the connector is working then writes the results to STDOUT
     */
    public function test()
    {
        $this->access_key    = $this->singer->config->input('access_key');
        $this->secret_key    = $this->singer->config->input('secret_key');
        $this->region        = $this->singer->config->input('region');

        try {
            $this->initializeS3Client();
            $this->singer->writeMeta(['test_result' => true]);
        } catch (Exception $e) {
            $this->singer->writeMeta(['test_result' => false]);
        }
    }

    /**
     * gets all schemas/tables and writes the results to STDOUT
     */
    public function discover()
    {
        $this->singer->logger->debug('Starting discover for tap Amazon S3');

        $this->access_key    = $this->singer->config->setting('access_key');
        $this->secret_key    = $this->singer->config->setting('secret_key');
        $this->region        = $this->singer->config->setting('region');
        $this->bucket_name   = $this->singer->config->setting('bucket_name');

        $this->initializeS3Client();

        foreach ($this->singer->config->catalog->streams as $stream) {
            $table = strtolower($stream->stream);

            $columns = $this->getTableColumns($table);

            $this->singer->logger->debug('columns', [$columns]);

            $this->singer->writeSchema(
                stream: $table,
                schema: $columns,
                key_properties: []
            );
        }
    }

    /**
     * gets the record data and writes to STDOUT
     */
    public function tap()
    {
        $this->singer->logger->debug('Starting sync for tap Amazon S3');

        $this->access_key    = $this->singer->config->setting('access_key');
        $this->secret_key    = $this->singer->config->setting('secret_key');
        $this->region        = $this->singer->config->setting('region');
        $this->bucket_name   = $this->singer->config->setting('bucket_name');

        $this->initializeS3Client();

        foreach ($this->singer->config->catalog->streams as $stream) {
            $table = $stream->stream;
            
            // Temporary: Do full replace until updateState function is fixed.
            $this->singer->logger->debug("Starting build for {$table}");

            $columns = $this->getTableColumns($table);
            $this->singer->writeSchema(
                stream: $table,
                schema: $columns,
                key_properties: []
            );
            ////

            $this->singer->logger->debug("Starting sync for {$table}");

            $bookmarks = $this->singer->config->state->bookmarks->$table;
            if ( isset($bookmarks->last_synced_key) ) {
                $start_after = $bookmarks->last_synced_key;
                if ( $start_after == "0001-01-01 00:00:00" ) {
                    $start_after = "";
                }
            } else {
                $start_after = "";
            }

            // Get all subdirectories in the data directory
            $prefixes = [];
            $this->listPrefixes(self::DATA_ROOT . $table . "/", $prefixes);
            $prefixes = array_values($prefixes);

            // Download objects
            $last_synced_key = $start_after;
            $total_records = 0;
            foreach ($prefixes as $prefix) {
                $this->singer->logger->debug("starting sync for {$prefix}");

                $objects = $this->listObjects($prefix, $start_after);

                foreach ($objects as $object) {
                    $record = $this->getObject($object);
                    $record['object_path'] = $object;

                    $record = $this->formatRecord((array) $record, $columns);

                    $this->singer->writeRecord(
                        stream: $table,
                        record: $record
                    );

                    $last_synced_key = $object;
                    $total_records++;
                }
            }

            /**
             * @todo connector updateState function accepts only datetime type last_started key
             * at /var/www/dev/app/Console/Commands/Connector/Run.php:912
             */
            // $this->singer->writeState([
            //     $table => [
            //         'last_synced_key' => $last_synced_key,
            //     ]
            // ]);

            $this->singer->writeMetric(
                'counter',
                'record_count',
                $total_records,
                [
                    'table' => $table
                ]
            );

            $this->singer->logger->debug("Finished sync for {$table}");
        }
    }

    /**
     * writes a metadata response with the tables to STDOUT
     */
    public function getTables()
    {
        $this->access_key    = $this->singer->config->input('access_key');
        $this->secret_key    = $this->singer->config->input('secret_key');
        $this->region        = $this->singer->config->input('region');
        $this->bucket_name   = $this->singer->config->input('bucket_name');

        $this->initializeS3Client();

        $prefix = self::SCHEMAS_ROOT;
        $objects = $this->listObjects($prefix);

        $tables = [];
        foreach ($objects as $object_path) {
            $object_name = str_replace($prefix, "", $object_path);
            $table_name = str_replace(".json", "", $object_name);

            $tables[] = $table_name;
        }

        $this->singer->writeMeta(compact('tables'));
    }

    /**
     * Initialize the Amazon S3 client
     *
     * @return void
     */
    public function initializeS3Client()
    {
        try {
            $this->client = new S3Client([
                'version'     => 'latest',
                'region'      => $this->region,
                'credentials' => [
                    'key'    => $this->access_key,
                    'secret' => $this->secret_key,
                ],
            ]);
        } catch (Exception $e) {
            throw new Exception("Failed to initialize the amazon s3 client. {$e->getMessage()}");
        }
    }

    /**
     * Recursively get a list of all subdirectories in the directory
     * @param string     $prefix                The parent directory path
     * @param array      $prefixes              The array of all subdirectories
     * @param string     $continuationToken     Next continuation token
     * @return array
     */
    public function listPrefixes($prefix, &$prefixes, $continuationToken = "")
    {
        try {
            $listObjectConfig = [
                '@retries'  => self::MAX_RETRIES,
                'Bucket'    => $this->bucket_name,
                'Delimiter' => '/',
                'ListType'  => 2,
                'Prefix'    => $prefix,
                'MaxKeys'   => 1000,
            ];
            if ($continuationToken) {
                $listObjectConfig['ContinuationToken'] = $continuationToken;
            }

            $data = $this->client->listObjectsV2($listObjectConfig);
        } catch (Exception $e) {
            throw new Exception("Failed to list objects in {$this->bucket_name} with error: " . $e->getMessage());
        }

        if ( isset($data['CommonPrefixes']) ) {
            if ( ! isset($data['Contents']) ) {
                unset($prefixes[array_search($prefix, $prefixes)]);
            }

            if ( isset($data['NextContinuationToken']) ) {
                $this->listPrefixes($prefix, $prefixes, $data['NextContinuationToken']);
            }

            foreach ($data['CommonPrefixes'] as $commonPrefix) {
                $prefixes[] = $commonPrefix['Prefix'];
                $this->listPrefixes($commonPrefix['Prefix'], $prefixes);
            }
        }

        return $prefixes;
    }

    /**
     * Get list of objects in directory.
     * @param string    $prefix             directory path
     * @param string    $start_after        starts listing after this specified key
     * @return array
     */
    public function listObjects($prefix, $start_after = "")
    {
        $objects = [];

        $listObjectConfig = [
            '@retries'  => self::MAX_RETRIES,
            'Bucket'    => $this->bucket_name,
            'Delimiter' => '/',
            'ListType'  => 2,
            'Prefix'    => $prefix,
            'MaxKeys'   => 1000,
        ];
        if ( $start_after ) {
            $listObjectConfig['StartAfter'] = $start_after;
        }

        $continuationToken = "";
        do {
            try {
                if ($continuationToken) {
                    $listObjectConfig['ContinuationToken'] = $continuationToken;
                }

                $data = $this->client->listObjectsV2($listObjectConfig);
            } catch (Exception $e) {
                throw new Exception("Failed to list objects in {$this->bucket_name} with error: " . $e->getMessage());
            }

            if ( isset($data['Contents']) ) {
                foreach ($data['Contents'] as $content) {
                    $key = (string) $content['Key'];
                    $objects[] = $key;
                }
            }

            if ( isset($data['NextContinuationToken']) ) {
                $continuationToken = $data['NextContinuationToken'];
            } else {
                $continuationToken = "";
            }
        } while ($continuationToken != "");

        return array_values($objects);
    }

    /**
     * Get S3 object data
     * @param  string    $key    Object key (full path)
     * @return array
     */
    public function getObject($key)
    {
        $getObjectConfig = [
            'Bucket' => $this->bucket_name,
            'Key' => $key
        ];

        $attempts = 0;
        while (True) {
            try {
                $object = $this->client->getObject($getObjectConfig);

                $body = $object->get('Body');
                $content = json_decode((string) $body);

                return (array) $content;
            } catch (Exception $e) {
                $attempts++;
                if ($attempts > self::MAX_RETRIES) {
                    throw new Exception("Failed to download {$key} from {$this->bucket_name} with error: " . $e->getMessage());
                }

                $this->singer->logger->debug("Getting {$key} object from {$this->bucket_name} failed. Will retry in " . self::RETRY_INTERVAL . " seconds.");
                sleep(self::RETRY_INTERVAL);
            }
        }
    }

    /**
     * Get the columns of a table based on schema and sample data files
     * @param  string  $table   The table name
     * @return array   Array of columns
     */
    public function getTableColumns($table)
    {
        // Read columns from the schema file
        $schema = $this->getObject(self::SCHEMAS_ROOT . $table . ".json");

        // Select the first data file (the data file has additional columns not included in the schema file)
        $prefixes = [];
        $this->listPrefixes(self::DATA_ROOT . $table . "/", $prefixes);
        $prefixes = array_values($prefixes);
        $objects = $this->listObjects($prefixes[0]);
        $object = $objects[0];

        // Read columns from the data file
        $data = $this->getObject($object);

        // Merge schema columns and data columns
        $columns = [];
        foreach ($schema as $key => $value) {
            $columns[$key] = [
                'type' => $this->getColumnType($value)
            ];
        }
        foreach ($data as $key => $value) {
            /**
             * A bug in the schema file. clients/data-lake/schemas/recurring_donation.json
             * defined "transaction_id": "number"
             * but transaction_id type is actually "string" in data files.
             * 
             * So we will overwrite schema column types with data column types.
             */
            // if ( ! array_key_exists($key, $columns) ) {
            //     $columns[$key] = [
            //         'type' => $this->getColumnType($value)
            //     ];
            // }
            $columns[$key] = [
                'type' => $this->getColumnType($value)
            ];
        }

        // Add a column for the full object path
        $columns['object_path'] = [
            'type' => $this->getColumnType('string')
        ];

        return $columns;
    }

    /**
     * Format the record based on the table column structure.
     * @param  array  $record   The record to format
     * @param  array  $columns  The table column
     * @return array  The formarted record
     */
    public function formatRecord($record, $columns)
    {
        $record = array_filter($record, function($key) use($columns) {
            return array_key_exists($key, $columns);
        }, ARRAY_FILTER_USE_KEY);

        foreach ($columns as $colKey => $colVal) {
            if ( !array_key_exists($colKey, $record) ) {
                $record[$colKey] = null;
            }
        }

        return $record;
    }

    /**
     * Attempt to determine the data type of a column based on its value
     * @param  mixed  $value The value of the column
     * @return string The postgres friendly data type to be used in the column definition
     */
    public function getColumnType($value)
    {
        $types = [
            'string'        => Singer::TYPE_STRING,
            'number'        => Singer::TYPE_NUMBER,
            'boolean'       => Singer::TYPE_BOOLEAN,
            'array'         => Singer::TYPE_ARRAY,
            'object'        => Singer::TYPE_OBJECT,
            'timestamp'     => Singer::TYPE_TIMESTAMP,
            'timestamp-tz'  => Singer::TYPE_TIMESTAMPTZ,
        ];

        if ( array_key_exists($value, $types) ) {
            return $types[$value];
        } else {
            $type = strtolower(gettype($value));
            return array_key_exists($type, $types) ? $types[$type] : Singer::TYPE_STRING;
        }
    }
}

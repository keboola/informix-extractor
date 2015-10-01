<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 22/09/15
 * Time: 15:00
 */

namespace Keboola\InformixExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\InformixExtractor\Exception\DbException;
use Symfony\Component\Yaml\Yaml;

class Extractor
{
    protected $dbConfig;
    protected $dataDir;

    public function __construct($dbParams, $dataDir = '/data')
    {
        $this->dbConfig = $dbParams;
        $this->dataDir = $dataDir;

        try {
            $this->conn = $this->createConnection($dbParams);
        } catch (\Exception $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new \Exception("Missing driver", 500, $e);
            }
            throw new DbException("Error connecting to DB: " . $e->getMessage(), $e);
        }
    }

    public function run($table)
    {
        $incremental = isset($table['incremental']) ? $table['incremental'] : false;
        $primaryKey = isset($table['primaryKey']) ? $table['primaryKey'] : null;

        $maxTries = isset($this->dbConfig['retries']) ? $this->dbConfig['retries']+1 : 1;
        if (is_numeric($maxTries) == false) {
            throw new DbException("Retries parameter is not a valid number, given:" . $maxTries);
        }

        $tries = 0;
        $isError = true;
        $dbException = null;
        while ($tries < $maxTries && $isError) {
            $isError = false;
            try {
                return $this->export($table['query'], $table['outputTable'], $incremental, $primaryKey);
            } catch (DbException $e) {
                print_r("Warning: " . $e->getMessage());
                $isError = true;
                $dbException = $e;
            }

            sleep(pow(2, $tries) + rand(0, 1000) / 1000);
            $tries++;
        }

        if ($isError) {
            throw new DbException(
                "Query '{$table['query']}' failed after {$maxTries} attempt(s). Reason '{$dbException->getMessage()}'",
                $dbException
            );
        }
    }

    /**
     * @param $dbParams
     * @return \PDO
     * @throws DbException
     */
    public function createConnection($dbParams)
    {
        // check params
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new DbException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '3306';
        $dbLocale = isset($dbParams['locale']) ? $dbParams['locale'] : 'en_US.819';

        $informixDsn = "informix:host=%s; service=%s; database=%s; server=%s; client_locale=en_us.utf8; db_locale=%s; protocol=onsoctcp; EnableScrollableCursors=1";

        $dsn = sprintf(
            $informixDsn,
            $dbParams['host'], $dbParams['port'], $dbParams['database'], $dbParams['server'], $port, $dbLocale
        );

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['password']);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    public function getConnection()
    {
        return $this->conn;
    }

    public function export($query, $outputTable, $incremental = false, $primaryKey = null)
    {
        $outFilename = $this->dataDir . "/out/tables/" . $outputTable . ".csv";
        $outManifestFilename = $this->dataDir . "/out/tables/" . $outputTable . ".csv.manifest";

        $csv = new CsvFile($outFilename);

        try {
            $stmt = $this->conn->prepare($query);
            $stmt->execute();
        } catch (\PDOException $e) {
            throw new DbException("DB query failed: " . $e->getMessage(), $e);
        }

        // write header and first line
        try {
            $resultRow = $stmt->fetch(\PDO::FETCH_ASSOC);
        } catch (\PDOException $e) {
            throw new DbException("DB query fetch failed: " . $e->getMessage(), $e);
        }

        if (is_array($resultRow) && !empty($resultRow)) {
            $csv->writeRow(array_keys($resultRow));
            $csv->writeRow($resultRow);

            // write the rest
            try {
                while ($resultRow = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                    $csv->writeRow($resultRow);
                }
            } catch (\PDOException $e) {
                throw new DbException("DB query fetch failed: " . $e->getMessage(), $e);
            } catch (\Exception $e) {
                // catch warnings
                throw new DbException("Db query fetch failed: " . $e->getMessage(), $e);
            }

            // write manifest
            file_put_contents($outManifestFilename, Yaml::dump([
                'destination' => $outputTable,
                'incremental' => $incremental,
                'primary_key' => $primaryKey
            ]));

        } else {
            print_r("Query returned empty result. Nothing was imported.");
        }

        return $csv;
    }
}

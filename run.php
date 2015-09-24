<?php

use Keboola\DbExtractorBundle\Exception\DbException;
use Keboola\InformixExtractor\Extractor\Extractor;
use Symfony\Component\Yaml\Yaml;

require_once(dirname(__FILE__) . "/bootstrap.php");

const APP_NAME = 'ex-informix';

$arguments = getopt("d::", array("data::"));
if (!isset($arguments["data"])) {
    print "Data folder not set.";
    exit(1);
}

try {
    $config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));

    $extractor = new Extractor($config['parameters']['db'], $arguments["data"]);

    foreach ($config['parameters']['queries'] as $query) {
        $csv = $extractor->run($query);
    }

} catch(DbException $e) {
    print_r($e->getMessage());
    exit(1);
} catch(Exception $e) {
    print_r($e->getMessage());
    exit(2);
}

print_r("Success");
exit(0);

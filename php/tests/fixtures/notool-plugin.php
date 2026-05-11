<?php

declare(strict_types=1);

/**
 * No-tool fixture: declares no tools and registers no tool handler, so
 * the initialize reply omits `tools` and any tool.invoke gets -32601.
 */

require __DIR__ . '/../../vendor/autoload.php';

use Nexo\Plugin\Sdk\PluginAdapter;

$MANIFEST = "[plugin]\n"
    . "id = \"notool_plugin\"\n"
    . "version = \"0.1.0\"\n"
    . "name = \"NoTool\"\n"
    . "description = \"fixture\"\n"
    . "min_nexo_version = \">=0.1.0\"\n";

$adapter = new PluginAdapter(['manifestToml' => $MANIFEST, 'handleProcessSignals' => false]);
$adapter->run();

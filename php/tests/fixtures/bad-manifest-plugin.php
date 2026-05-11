<?php

declare(strict_types=1);

/**
 * Drift fixture: declares a tool whose name is NOT in the manifest's
 * [plugin.extends].tools, so the PluginAdapter constructor throws a
 * ManifestError before the handshake — the process exits non-zero.
 */

require __DIR__ . '/../../vendor/autoload.php';

use Nexo\Plugin\Sdk\PluginAdapter;
use Nexo\Plugin\Sdk\ToolDef;

$MANIFEST = "[plugin]\n"
    . "id = \"bad_plugin\"\n"
    . "version = \"0.1.0\"\n"
    . "name = \"Bad\"\n"
    . "description = \"fixture\"\n"
    . "min_nexo_version = \">=0.1.0\"\n\n"
    . "[plugin.extends]\n"
    . "tools = [\"bad_plugin_known\"]\n";

$adapter = new PluginAdapter([
    'manifestToml' => $MANIFEST,
    'handleProcessSignals' => false,
    'tools' => [new ToolDef('bad_plugin_unknown', 'x')],
    'onTool' => static fn ($inv): array => [],
]);
$adapter->run();

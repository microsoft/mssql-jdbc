#Requires -Version 7.0
<#
.SYNOPSIS
Build, run, verify or stop the isolated local JDBC automatic-OTel demo.
.DESCRIPTION
Start requires explicit SQL Server Developer license acceptance, Docker with local
Linux x64 containers, JDK 21 (java/javac/jar) and Maven. No host Node, cloud account,
test configuration or preinstalled SQL Server is used. Start builds both packaged
JARs and the Docker UI, starts the stack, runs Workload, then performs Verify.
Workload reuses only the manifest-owned SQL container and packaged build outputs.
Verify and Stop require Docker only, not Java, Maven, license acceptance or SQL secrets.

Default state: <checkout>/target/user-endpoint-demo/<Project>. State contains no
secrets: ownership manifest, dependency classpath and compiled harness. Keep the
manifest until Stop completes. A checkout/state directory is trusted executable
input, not a security boundary against a malicious local user or Docker admin.

All published ports bind IPv4 loopback. SQL uses a generated process-environment
password, visible to Docker administrators/container inspect while the container
exists. Never enable shell transcription/debug tracing or dump container env.
SQL uses demo-only trustServerCertificate=true for its local self-signed certificate.
No SQL objects are created; constant reads, deliberate syntax errors and a 2s
WAITFOR are executed. Version-pinned historical images are not production approved.
Stop removes only the owned Compose project; -RemoveVolumes also removes its
anonymous volumes. State/build outputs and shared image/Maven caches remain.

Scrapes are 5s apart; iterations wait 6s plus slow SQL. Rates and histogram p95
are sparse/bucket estimates, not exact SQL timings. Flush is not a delivery ACK.
Verification reports observed API evidence, not losslessness/privacy certification.
.PARAMETER Action
Start, Workload, Verify or Stop. Start is the default.
.PARAMETER AcceptSqlServerLicense
Required only for Start; accepts the SQL Server Developer container EULA.
.PARAMETER Project
Unique lower-case Compose project name. Fixed ports must still be free.
.PARAMETER StateDirectory
Optional dedicated state directory. Must match the manifest on subsequent calls.
.PARAMETER Iterations
Workload iterations, 1 to 100; default 5. Each executes five statement roots.
.PARAMETER RemoveVolumes
Stop only: remove this project's anonymous volumes, never shared caches.
.EXAMPLE
./run.ps1 -Action Start -AcceptSqlServerLicense
.EXAMPLE
./run.ps1 -Action Workload -Iterations 10
.EXAMPLE
./run.ps1 -Action Verify
.EXAMPLE
./run.ps1 -Action Stop -RemoveVolumes
#>
[CmdletBinding()]
param(
    [ValidateSet('Start', 'Workload', 'Verify', 'Stop')][string] $Action = 'Start',
    [switch] $AcceptSqlServerLicense,
    [ValidatePattern('^[a-z0-9][a-z0-9_-]{0,47}$')][string] $Project = 'jdbc-user-endpoint-demo',
    [string] $StateDirectory,
    [ValidateRange(1, 100)][int] $Iterations = 5,
    [switch] $RemoveVolumes
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# Native errors are checked explicitly and never echoed with potentially sensitive output.
$PSNativeCommandUseErrorActionPreference = $false
$Repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../../..'))
$ComposeFile = Join-Path $PSScriptRoot 'compose.yaml'
if (!$StateDirectory) { $StateDirectory = Join-Path $Repo "target/user-endpoint-demo/$Project" }
$StateDirectory = [IO.Path]::GetFullPath($StateDirectory)
$ManifestPath = Join-Path $StateDirectory 'manifest.json'
$Services = @('sql', 'collector', 'prometheus', 'jaeger', 'grafana', 'dashboard')
$Ports = @(14333, 4318, 8889, 13133, 9090, 16686, 3000, 3002)
$OwnerLabel = 'io.mssql.jdbc.demo.owner'
$SavedEnvironment = @{}
$Lock = $null
$State = $null

function Set-DemoEnvironment([string] $Name, [AllowNull()][string] $Value) {
    if (!$SavedEnvironment.ContainsKey($Name)) {
        $SavedEnvironment[$Name] = [Environment]::GetEnvironmentVariable($Name, 'Process')
    }
    [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
}

function Invoke-Checked([string] $File, [string[]] $Arguments, [string] $Stage) {
    $output = & $File @Arguments 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "$Stage failed (exit $LASTEXITCODE); native output suppressed for privacy." }
    return $output.Trim()
}

function Invoke-Compose([string[]] $ComposeArguments) {
    # Disable implicit .env loading and inherited Compose overrides/profiles.
    Invoke-Checked 'docker' (@('compose', '--env-file', (Join-Path $PSScriptRoot 'empty.env'),
        '--project-directory', $PSScriptRoot, '-p', $Project, '-f', $ComposeFile) + $ComposeArguments) 'Compose'
}

function Get-ProjectContainers {
    $text = Invoke-Checked 'docker' @('ps', '-aq', '--filter', "label=com.docker.compose.project=$Project") 'Container discovery'
    return @($text -split '\r?\n' | Where-Object { $_ })
}

function Assert-OwnedResources {
    foreach ($id in @(Get-ProjectContainers)) {
        $labels = (Invoke-Checked 'docker' @('inspect', '--format', '{{json .Config.Labels}}', $id) 'Ownership inspection') |
            ConvertFrom-Json -AsHashtable
        if ($labels[$OwnerLabel] -cne $State.owner -or
            $labels['com.docker.compose.project'] -cne $Project -or
            $labels['com.docker.compose.service'] -cnotin $Services -or
            $labels['com.docker.compose.oneoff'] -eq 'True') {
            throw 'Project contains a container not owned by this manifest; refusing to operate.'
        }
    }
    foreach ($kind in @('network', 'volume')) {
        $text = Invoke-Checked 'docker' @($kind, 'ls', '-q', '--filter', "label=com.docker.compose.project=$Project") 'Resource discovery'
        foreach ($id in @($text -split '\r?\n' | Where-Object { $_ })) {
            $labels = (Invoke-Checked 'docker' @($kind, 'inspect', '--format', '{{json .Labels}}', $id) 'Resource ownership inspection') |
                ConvertFrom-Json -AsHashtable
            if ($null -eq $labels -or $labels[$OwnerLabel] -cne $State.owner) {
                throw 'Project contains a network or volume not owned by this manifest; refusing to operate.'
            }
        }
    }
    # Also guard the exact default network name when it lacks Compose project labels.
    $network = Invoke-Checked 'docker' @('network', 'ls', '-q', '--filter', "name=^${Project}_default$") 'Default network discovery'
    if ($network) {
        $labels = (Invoke-Checked 'docker' @('network', 'inspect', '--format', '{{json .Labels}}', $network) 'Default network ownership inspection') |
            ConvertFrom-Json -AsHashtable
        if ($null -eq $labels -or $labels[$OwnerLabel] -cne $State.owner) {
            throw 'Default network name is already owned by another stack; refusing to operate.'
        }
    }
}

function Get-OwnedSql {
    Assert-OwnedResources
    $ids = @((Invoke-Compose @('ps', '-q', 'sql')) -split '\r?\n' | Where-Object { $_ })
    if ($ids.Count -ne 1) { throw 'Exactly one running, manifest-owned SQL container is required.' }
    return $ids[0]
}

function Save-Manifest {
    # No secrets; atomic same-directory replacement, protected against concurrent runner calls.
    $temporary = Join-Path $StateDirectory 'manifest.pending.json'
    [IO.File]::WriteAllText($temporary, ($State | ConvertTo-Json) + "`n", [Text.UTF8Encoding]::new($false))
    [IO.File]::Move($temporary, $ManifestPath, $true)
}

function Wait-Until([scriptblock] $Test, [int] $Seconds, [string] $Description) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        if (& $Test) { return }
        Start-Sleep -Seconds 3
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for $Description. Stack preserved; inspect locally, then use -Action Stop."
}

function Get-LocalJson([string] $Url) {
    Invoke-RestMethod -Uri $Url -TimeoutSec 5 -NoProxy
}

function Assert-Health {
    $null = Get-LocalJson 'http://127.0.0.1:13133/'
    $null = Invoke-WebRequest 'http://127.0.0.1:9090/-/ready' -TimeoutSec 5 -NoProxy
    $grafana = Get-LocalJson 'http://127.0.0.1:3000/api/health'
    if ($grafana.database -ne 'ok') { throw 'Grafana is not ready.' }
    $null = Get-LocalJson 'http://127.0.0.1:16686/api/services'
    $null = Invoke-WebRequest 'http://127.0.0.1:3002/healthz' -TimeoutSec 5 -NoProxy
    $null = Invoke-WebRequest 'http://127.0.0.1:3002/' -TimeoutSec 5 -NoProxy
    $null = Get-LocalJson 'http://127.0.0.1:3002/jaeger/api/services'
    $proxy = Get-LocalJson 'http://127.0.0.1:3002/prometheus/api/v1/query?query=up'
    if ($proxy.status -ne 'success') { throw 'UI Prometheus proxy is not ready.' }
}

function Assert-Jdk {
    foreach ($tool in @('java', 'javac', 'jar')) { $null = Get-Command $tool -ErrorAction Stop }
    $java = Invoke-Checked 'java' @('-XshowSettings:properties', '-version') 'JDK inspection'
    $javac = Invoke-Checked 'javac' @('-version') 'Compiler inspection'
    if ($java -notmatch '(?m)^\s*java\.version\s*=\s*21(?:\.|\s|$)' -or $javac -notmatch '^javac 21(?:\.|\s|$)') {
        throw 'JDK 21 java and javac are required. Set JAVA_HOME/PATH explicitly; nothing will be installed.'
    }
}

function Get-RuntimeClasspath {
    $classpathFile = Join-Path $StateDirectory 'deps.cp'
    $adapterPom = [xml][IO.File]::ReadAllText((Join-Path $Repo 'mssql-jdbc-otel/pom.xml'))
    $version = [string]$adapterPom.project.version
    if ($version -notmatch '^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$') { throw 'Unexpected adapter artifact version.' }
    $adapter = Join-Path $Repo "mssql-jdbc-otel/target/mssql-jdbc-otel-$version.jar"
    if (!(Test-Path -LiteralPath $adapter -PathType Leaf) -or !(Test-Path -LiteralPath $classpathFile -PathType Leaf)) {
        throw 'Packaged artifacts/classpath missing; run Start after stopping this project.'
    }
    $dependencies = [IO.File]::ReadAllText($classpathFile).Trim()
    foreach ($entry in $dependencies.Split([IO.Path]::PathSeparator)) {
        if (!$entry.EndsWith('.jar') -or !(Test-Path -LiteralPath $entry -PathType Leaf)) {
            throw 'Dependency classpath must contain existing packaged JARs only; rebuild with Start.'
        }
    }
    return $adapter + [IO.Path]::PathSeparator + $dependencies
}

function Build-Demo {
    Assert-Jdk
    $null = Get-Command mvn -ErrorAction Stop
    $settings = Join-Path $Repo 'docs/otel-poc/settings.xml'
    $adapterPom = Join-Path $Repo 'mssql-jdbc-otel/pom.xml'
    Write-Host 'Building packaged core and optional adapter (integration tests intentionally skipped)...'
    $null = Invoke-Checked 'mvn' @('-B', '-s', $settings, '-f', (Join-Path $Repo 'pom.xml'), '-Pjre21',
        '-Dmaven.test.skip=true', '-Dmaven.javadoc.skip=true', 'install') 'Core build'
    $null = Invoke-Checked 'mvn' @('-B', '-s', $settings, '-f', $adapterPom,
        '-Dmaven.test.skip=true', 'package') 'Adapter build'
    $null = Invoke-Checked 'mvn' @('-B', '-s', $settings, '-f', $adapterPom,
        'org.apache.maven.plugins:maven-dependency-plugin:3.6.1:build-classpath', '-DincludeScope=test',
        "-Dmdep.outputFile=$(Join-Path $StateDirectory 'deps.cp')") 'Classpath build'
    $classpath = Get-RuntimeClasspath
    $adapter = $classpath.Split([IO.Path]::PathSeparator)[0]
    $contents = Invoke-Checked 'jar' @('tf', $adapter) 'SPI descriptor inspection'
    if ($contents -notmatch '(?m)^META-INF/services/com\.microsoft\.sqlserver\.jdbc\.UserOtelTelemetryProvider\r?$') {
        throw 'Packaged automatic provider SPI descriptor is missing.'
    }
    $classes = Join-Path $StateDirectory 'classes'
    $null = [IO.Directory]::CreateDirectory($classes)
    $null = Invoke-Checked 'javac' @('-cp', $classpath, '-d', $classes,
        (Join-Path $PSScriptRoot 'UserEndpointDemo.java')) 'Harness compilation'
}

function Invoke-Workload {
    Assert-Jdk
    $classpath = Get-RuntimeClasspath
    if (!(Test-Path -LiteralPath (Join-Path $StateDirectory 'classes/UserEndpointDemo.class') -PathType Leaf)) {
        throw 'Compiled harness missing; rebuild with Start.'
    }
    $sql = Get-OwnedSql
    # The command line contains only the variable name. Never inspect/dump all Config.Env.
    $password = Invoke-Checked 'docker' @('exec', $sql, 'printenv', 'MSSQL_SA_PASSWORD') 'Owned SQL password retrieval'
    try {
        if ([string]::IsNullOrWhiteSpace($password)) { throw 'Owned SQL password is unavailable.' }
        Set-DemoEnvironment 'JDBC_DEMO_PASSWORD' $password
        Set-DemoEnvironment 'DEMO_MODE' 'workload'
        Set-DemoEnvironment 'DEMO_ITERATIONS' ([string]$Iterations)
        $State.workloadStartMicroseconds = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() * 1000
        $State.workloadCompleted = $false
        Save-Manifest
        Write-Host "Running $Iterations local SQL iterations, including config/DNS and prepared-statement failures..."
        $output = Invoke-Checked 'java' @('-cp', ((Join-Path $StateDirectory 'classes') + [IO.Path]::PathSeparator + $classpath),
            'UserEndpointDemo') 'Local workload'
        # Only known-safe harness evidence is printed, never arbitrary SDK/JDBC stderr.
        $output -split '\r?\n' | Where-Object {
            $_ -match '^(Automatic provider SPI present;|Expected (DNS connection failure observed|port configuration failure observed|syntax error code: 102)$|Completed connection,|Flush completed \(not delivery ACK\): (true|false)$)'
        } | ForEach-Object { Write-Host $_ }
        $State.workloadCompleted = $true
        Save-Manifest
    } finally {
        Set-DemoEnvironment 'JDBC_DEMO_PASSWORD' $null
        $password = $null
    }
}

function Get-Tag($Tags, [string] $Name) {
    $values = @($Tags | Where-Object { $_.key -eq $Name })
    if ($values.Count -gt 0) { return $values[0].value }
    return $null
}

function Test-SpanError($Span) {
    return (Get-Tag $Span.tags 'otel.status_code') -eq 'ERROR' -or (Get-Tag $Span.tags 'error') -eq $true
}

function Get-RootMetric([string] $Family, [string] $Activity) {
    $kind = if ($Activity -eq 'CONNECTION') { 'connection' } else { 'statement' }
    $query = "sum(${Family}{job=`"jdbc-user-endpoint`",mssql_activity=`"$Activity`",mssql_activity_type=`"$kind`"})"
    $response = Get-LocalJson ('http://127.0.0.1:9090/api/v1/query?query=' + [uri]::EscapeDataString($query))
    if ($response.status -ne 'success' -or @($response.data.result).Count -ne 1) { throw 'Root metric not observed.' }
    $value = [double]::Parse([string]$response.data.result[0].value[1], [Globalization.CultureInfo]::InvariantCulture)
    if ([double]::IsNaN($value) -or [double]::IsInfinity($value) -or $value -le 0) { throw 'Root metric not positive.' }
    return $value
}

function Assert-Evidence {
    Assert-Health
    $exposition = (Invoke-WebRequest 'http://127.0.0.1:8889/metrics' -TimeoutSec 5 -NoProxy).Content
    foreach ($family in @('db_client_operation_count_total', 'db_client_operation_error_count_total',
        'db_client_operation_duration_seconds_bucket', 'db_client_operation_duration_seconds_sum',
        'db_client_operation_duration_seconds_count')) {
        if ($exposition -notmatch "(?m)^$family\{") { throw 'Expected translated JDBC metric family is missing.' }
    }
    $counts = @{}
    foreach ($activity in @('CONNECTION', 'STATEMENT_INVOCATION')) {
        $counts[$activity] = Get-RootMetric 'db_client_operation_count_total' $activity
        $null = Get-RootMetric 'db_client_operation_error_count_total' $activity
    }
    $traces = Get-LocalJson ("http://127.0.0.1:16686/api/traces?service=mssql-jdbc&limit=1000&start=" +
        $State.workloadStartMicroseconds)
    $roots = @($traces.data | ForEach-Object { $_.spans } | Where-Object {
        @($_.references).Count -eq 0 -and $_.startTime -ge $State.workloadStartMicroseconds
    })
    foreach ($operation in @('mssql.driver.connection.open', 'mssql.driver.statement.execute')) {
        $matching = @($roots | Where-Object { $_.operationName -eq $operation })
        if (@($matching | Where-Object { Test-SpanError $_ }).Count -eq 0 -or
            @($matching | Where-Object { !(Test-SpanError $_) }).Count -eq 0) {
            throw 'Successful and failed connection/statement roots have not both arrived.'
        }
    }
    $statements = @($roots | Where-Object { $_.operationName -eq 'mssql.driver.statement.execute' })
    if (@($statements | Where-Object { $_.duration -gt 1000000 -and !(Test-SpanError $_) }).Count -eq 0) {
        throw 'A measured successful statement root longer than one second is required.'
    }
    foreach ($kind in @('statement', 'prepared_statement')) {
        $typed = @($statements | Where-Object { (Get-Tag $_.tags 'mssql.statement.type') -eq $kind })
        if (@($typed | Where-Object { !(Test-SpanError $_) }).Count -eq 0) { throw 'Statement kind success is missing.' }
        $failed = @($typed | Where-Object { Test-SpanError $_ })
        $origin = $false
        foreach ($root in $failed) {
            $tree = @($traces.data | Where-Object { $_.traceID -eq $root.traceID })
            foreach ($span in @($tree | ForEach-Object { $_.spans })) {
                foreach ($log in @($span.logs)) {
                    if ((Get-Tag $log.fields 'event') -eq 'mssql.driver.error' -and
                        (Get-Tag $log.fields 'mssql.error.code') -eq 'sqlserver:102') { $origin = $true }
                }
            }
        }
        if (!$origin) { throw 'Failed statement kind with SQL error 102 origin event is missing.' }
    }
    foreach ($phase in @('configuration', 'dns')) {
        $found = @($traces.data | ForEach-Object { $_.spans } | Where-Object {
            $_.operationName -eq "mssql.driver.connection.$phase" -and (Test-SpanError $_) -and
            @($_.logs | Where-Object { (Get-Tag $_.fields 'event') -eq 'mssql.driver.error' }).Count -gt 0
        })
        if ($found.Count -eq 0) { throw 'Controlled connection error phase/origin evidence is missing.' }
    }
    Write-Host ("Verified fresh roots: {0}; observed cumulative connection/statement counters: {1}/{2}" -f
        $roots.Count, $counts.CONNECTION, $counts.STATEMENT_INVOCATION)
    Write-Host ('Safe root trace IDs: ' + (($roots | Select-Object -ExpandProperty traceID -Unique) -join ', '))
}

try {
    if ($RemoveVolumes -and $Action -ne 'Stop') { throw '-RemoveVolumes is valid only with Stop.' }
    if ($Action -eq 'Start' -and !$AcceptSqlServerLicense) {
        throw 'Start requires -AcceptSqlServerLicense to accept the SQL Server Developer container EULA.'
    }
    if ($Action -in @('Start', 'Workload') -and $env:JAVA_HOME) {
        # Honor caller-selected JAVA_HOME, restoring PATH on exit.
        Set-DemoEnvironment 'PATH' ((Join-Path $env:JAVA_HOME 'bin') + [IO.Path]::PathSeparator + $env:PATH)
    }
    $null = Get-Command docker -ErrorAction Stop
    $null = Invoke-Checked 'docker' @('compose', 'version') 'Docker Compose v2 preflight'
    if ($env:DOCKER_HOST -and !$env:DOCKER_CONTEXT) { $dockerHost = $env:DOCKER_HOST } else {
        $dockerHost = (Invoke-Checked 'docker' @('context', 'inspect', '--format', '{{json .Endpoints.docker.Host}}') 'Docker context inspection') |
            ConvertFrom-Json
    }
    if ($dockerHost -notmatch '^(unix://|npipe://)') { throw 'Only a local Unix socket/named-pipe Docker engine is supported.' }
    $server = (Invoke-Checked 'docker' @('info', '--format', '{{json .}}') 'Docker engine preflight') | ConvertFrom-Json
    if ($server.OSType -ne 'linux') { throw 'Switch Docker to Linux containers; Windows containers are unsupported.' }
    if ($Action -in @('Start', 'Workload') -and
        ([Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne [Runtime.InteropServices.Architecture]::X64 -or
        $server.Architecture -notin @('x86_64', 'amd64'))) {
        throw 'SQL Server Linux containers require native x64. ARM/Apple Silicon emulation is unsupported; use an x64 host.'
    }
    foreach ($name in @('COMPOSE_FILE', 'COMPOSE_PROFILES', 'COMPOSE_PROJECT_NAME', 'COMPOSE_ENV_FILES')) {
        Set-DemoEnvironment $name $null
    }
    if (!(Test-Path -LiteralPath $StateDirectory)) {
        if ($Action -ne 'Start') { throw 'State directory missing; refusing to adopt an existing Docker project.' }
        $null = [IO.Directory]::CreateDirectory($StateDirectory)
    }
    if ((Get-Item -LiteralPath $StateDirectory).Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw 'State directory must not be a symbolic link/reparse point.'
    }
    $Lock = [IO.File]::Open((Join-Path $StateDirectory 'runner.lock'), 'OpenOrCreate', 'ReadWrite', 'None')
    if (Test-Path -LiteralPath $ManifestPath -PathType Leaf) {
        $State = [IO.File]::ReadAllText($ManifestPath) | ConvertFrom-Json -AsHashtable
        if ($State.version -ne 1 -or $State.project -cne $Project -or $State.repo -cne $Repo -or
            $State.composeFile -cne $ComposeFile -or $State.stateDirectory -cne $StateDirectory -or
            $State.owner -notmatch '^[a-f0-9]{32}$') {
            throw 'Manifest project/checkout/ownership mismatch; refusing to operate.'
        }
    } elseif ($Action -ne 'Start') { throw 'Ownership manifest missing; refusing to adopt an existing project.' }
    if ($Action -eq 'Start') {
        # Never take over even a stopped project or a pre-existing network/volume.
        foreach ($kind in @('container', 'network', 'volume')) {
            $argsForList = if ($kind -eq 'container') { @('ps', '-aq') } else { @($kind, 'ls', '-q') }
            $existing = Invoke-Checked 'docker' ($argsForList + @('--filter', "label=com.docker.compose.project=$Project")) 'Project collision preflight'
            if ($existing) { throw 'Compose project already has resources. Use its matching manifest and Stop first.' }
        }
        $network = Invoke-Checked 'docker' @('network', 'ls', '-q', '--filter', "name=^${Project}_default$") 'Default network collision preflight'
        if ($network) { throw 'Default network name is occupied. No existing network was removed.' }
        foreach ($port in $Ports) {
            $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $port)
            try { $listener.Start() } catch { throw "Host port $port is occupied. No existing process/container was stopped." }
            finally { $listener.Stop() }
        }
        if ($null -eq $State) {
            $otherFiles = @(Get-ChildItem -LiteralPath $StateDirectory -Force | Where-Object { $_.Name -ne 'runner.lock' })
            if ($otherFiles.Count -gt 0) { throw 'New state directory must be empty; refusing to overwrite unrelated files.' }
        }
        $State = @{ version = 1; project = $Project; repo = $Repo; composeFile = $ComposeFile; stateDirectory = $StateDirectory;
            owner = [guid]::NewGuid().ToString('N'); workloadStartMicroseconds = 0; workloadCompleted = $false }
        Save-Manifest
    }
    Set-DemoEnvironment 'JDBC_DEMO_OWNER_ID' $State.owner
    # Non-Start actions only interpolate a dummy; they never recreate SQL.
    Set-DemoEnvironment 'JDBC_DEMO_SQL_PASSWORD' ('D3mo!' + [guid]::NewGuid().ToString('N'))
    $null = Invoke-Compose @('config', '--quiet')
    if ($Action -ne 'Start') { Assert-OwnedResources }
    switch ($Action) {
        'Start' {
            Build-Demo
            Write-Host 'Starting local SQL and monitoring; pinned image pulls/UI build may take several minutes...'
            $null = Invoke-Compose @('up', '--build', '-d')
            # Do not keep the SQL secret in the runner environment once Compose has consumed it.
            Set-DemoEnvironment 'JDBC_DEMO_SQL_PASSWORD' 'not-used-for-existing-containers'
            $sql = Get-OwnedSql
            Wait-Until {
                $logs = Invoke-Checked 'docker' @('logs', '--tail', '200', $sql) 'SQL readiness inspection'
                return $logs -match 'SQL Server is now ready for client connections'
            } 180 'SQL readiness (180s budget)'
            Wait-Until { try { Assert-Health; return $true } catch { return $false } } 120 'monitoring and UI health'
            Invoke-Workload
            Wait-Until { try { Assert-Evidence; return $true } catch { return $false } } 120 'driver telemetry evidence'
        }
        'Workload' {
            Assert-Health
            Invoke-Workload
            Wait-Until { try { Assert-Evidence; return $true } catch { return $false } } 120 'driver telemetry evidence'
        }
        'Verify' {
            $null = Get-OwnedSql
            if (!$State.workloadCompleted) { throw 'No completed workload in manifest; run Workload first.' }
            Wait-Until { try { Assert-Evidence; return $true } catch { return $false } } 120 'driver telemetry evidence'
        }
        'Stop' {
            $down = @('down')
            if ($RemoveVolumes) { $down += '--volumes' }
            $null = Invoke-Compose $down
            Write-Host 'Owned project stopped. State, build outputs and shared image/Maven caches retained.'
        }
    }
    if ($Action -ne 'Stop') {
        Write-Host 'Dashboard http://127.0.0.1:3002 | Grafana http://127.0.0.1:3000/d/jdbc-user-endpoint | Jaeger http://127.0.0.1:16686'
    }
} catch {
    # Controlled errors are safe; do not expose arbitrary native/API/exception payloads.
    $message = $_.Exception.Message
    if ($_.Exception -is [Management.Automation.RuntimeException] -and $_.FullyQualifiedErrorId -like '*WriteErrorException*') {
        $message = 'Demo action failed.'
    }
    Write-Warning "Demo action $Action did not complete. Owned resources, if any, are preserved; use the same Project/StateDirectory with Stop."
    throw $message
} finally {
    foreach ($entry in $SavedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    if ($null -ne $Lock) { $Lock.Dispose() }
}
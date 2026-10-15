param(
    [string]$Go = 'go',
    [Parameter(Mandatory = $true)][string]$Zig,
    [string[]]$Packages = @('./factor', './factor/runner', './runtime', './data', './execution', './entry', './biz', './opt'),
    [string]$Tests = '.',
    [string]$OutputDir = 'tmp/architecture-race',
    [string]$Timeout = '10m'
)

$ErrorActionPreference = 'Stop'
$previousCGO = $env:CGO_ENABLED
$previousCC = $env:CC
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$output = [IO.Path]::GetFullPath((Join-Path $root $OutputDir))
[IO.Directory]::CreateDirectory($output) | Out-Null
Push-Location $root
try {
    $env:CGO_ENABLED = '1'
    $env:CC = '"' + (Resolve-Path $Zig).Path + '" cc -target x86_64-windows-gnu -lapi-ms-win-core-synch-l1-2-0'
    foreach ($package in $Packages) {
        $name = $package.Replace('./', '').Replace('/', '-')
        $binary = Join-Path $output ($name + '.test.exe')
        $log = Join-Path $output ($name + '.log')
        [IO.File]::WriteAllText($log, '')
        # Linkers may emit a harmless diagnostic on stderr with exit code 0.
        # PowerShell 5 must judge compilation by its process result, just as it
        # does the negative test cases below, not NativeCommandError records.
        $compileErrorPolicy = $ErrorActionPreference
        try {
            $ErrorActionPreference = 'Continue'
            & $Go test -race -c $package -o $binary 2>&1 | Tee-Object -FilePath $log
            $compileExit = $LASTEXITCODE
        } finally { $ErrorActionPreference = $compileErrorPolicy }
        if ($compileExit -ne 0) { throw "Race compilation failed for $package; see $log" }

        # Zig/LLD's ASLR image placement can map Windows TSan shadow above
        # supported address space (VirtualAlloc error 87). Use a fixed image
        # base for this temporary instrumented test binary only. No machine
        # policy, Go runtime, project source, or production binary is changed.
        $bytes = [IO.File]::ReadAllBytes($binary)
        $peOffset = [BitConverter]::ToInt32($bytes, 0x3c)
        if ([BitConverter]::ToUInt32($bytes, $peOffset) -ne 0x4550 -or
            [BitConverter]::ToUInt16($bytes, $peOffset + 24) -ne 0x20b) {
            throw "Expected Windows PE32+ race test binary: $binary"
        }
        $flagOffset = $peOffset + 24 + 70
        $flags = [BitConverter]::ToUInt16($bytes, $flagOffset)
        $fixedFlags = [BitConverter]::GetBytes([uint16]($flags -band 0xff9f))
        $bytes[$flagOffset] = $fixedFlags[0]
        $bytes[$flagOffset + 1] = $fixedFlags[1]
        [IO.File]::WriteAllBytes($binary, $bytes)
        "Race package $package; temporary PE ASLR flags $flags -> $($flags -band 0xff9f)" | Tee-Object -FilePath $log -Append
        # Match go test's per-package working directory for relative fixtures.
        Push-Location (Join-Path $root $package)
        try {
            # Negative CLI tests intentionally write stderr. Windows
            # PowerShell must use the process exit code, not turn expected
            # stderr into a terminating NativeCommandError.
            $testErrorPolicy = $ErrorActionPreference
            try {
                $ErrorActionPreference = 'Continue'
                & $binary "-test.run=$Tests" '-test.count=1' "-test.timeout=$Timeout" '-test.v' 2>&1 | Tee-Object -FilePath $log -Append
                $testExit = $LASTEXITCODE
            } finally { $ErrorActionPreference = $testErrorPolicy }
            if ($testExit -ne 0) { throw "Race tests failed for $package; see $log" }
        } finally { Pop-Location }
    }
} finally {
    $env:CGO_ENABLED = $previousCGO
    $env:CC = $previousCC
    Pop-Location
}

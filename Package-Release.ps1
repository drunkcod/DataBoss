function Args {
    param([parameter(ValueFromRemainingArguments=$true)] [String[]] $args)
    $args
}

$BuildArgs = Args /v:m /nologo /p:Configuration=Release
$PackArgs = $BuildArgs + (Args /t:Pack /p:PackageOutputPath="..\Build")

function NuPack {
    param([parameter(ValueFromRemainingArguments=$true)] [String[]] $args)
    & dotnet msbuild "$args/$args.csproj" $PackArgs
}

dotnet msbuild ((Args DataBoss\DataBoss.sln /t:Restore /t:Clean /t:Build) + $BuildArgs)
NuPack DataBoss.Linq
NuPack DataBoss.Data 
NuPack DataBoss.Data.MsSql 
NuPack DataBoss.Data.SqlClient 
NuPack DataBoss.DataPackage
NuPack DataBoss

#CLI Packaging
$v = (Get-ChildItem Build\DataBoss.Cli\Release\net452\DataBoss.dll | Select-Object -ExpandProperty VersionInfo).ProductVersion
$p = "Build\Bin\DastaBoss.Cli-$v"

& { param($p)
    If (Test-Path $p) { Remove-Item $p -Recurse -Force }
} Build\Bin
dotnet publish DataBoss.Cli\DataBoss.Cli.csproj --nologo --no-build --configuration Release /p:TargetFramework=netcoreapp2.1 -o $p\netcoreapp2.1
Copy-Item Build\DataBoss.Cli\Release\net452 -Destination $p -Recurse -Force
$$ = @{
    Path = $p
    DestinationPath = "$p.zip"
}
Compress-Archive @$ -Force

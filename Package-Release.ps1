function Args {
    param([parameter(ValueFromRemainingArguments=$true)] [String[]] $args)
    $args
}

$BuildArgs = Args /v:m /nologo /p:Configuration=Release
$PackArgs = $BuildArgs + (Args /t:Pack /p:PackageOutputPath="..\Build")

function MSBuild {
    param([parameter(ValueFromRemainingArguments=$true)] [String[]] $args)
    & dotnet msbuild ($args + $BuildArgs)
}

function NuPack {
    param([parameter(ValueFromRemainingArguments=$true)] [String[]] $args)
    & dotnet msbuild ($args + $PackArgs) 
}

dotnet msbuild  DataBoss\DataBoss.sln /t:Restore /t:Clean /t:Build
NuPack DataBoss.Linq/DataBoss.Linq.csproj /t:Restore
NuPack DataBoss.Data/DataBoss.Data.csproj /t:Restore
NuPack DataBoss\DataBoss.csproj /t:Restore
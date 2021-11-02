function Args {
    param([parameter(ValueFromRemainingArguments=$true)] [String[]] $args)
    $args
}

$BuildArgs = Args /v:m /nologo /p:Configuration=Release
$PackArgs = $BuildArgs + (Args /t:Pack /p:PackageOutputPath="..\..\Build")

function NuPack {
    param([parameter(ValueFromRemainingArguments=$true)] [String[]] $args)
    & dotnet msbuild "Source/$args/$args.csproj" $PackArgs
}

dotnet msbuild ((Args DataBoss.sln /t:Restore /t:Clean /t:Build) + $BuildArgs)
NuPack DataBoss.Linq
NuPack DataBoss.PowerArgs
NuPack DataBoss.Data 
NuPack DataBoss.Data.MsSql 
NuPack DataBoss.Data.SqlClient 
NuPack DataBoss.DataPackage
NuPack DataBoss
NuPack DataBoss.Testing.SqlServer
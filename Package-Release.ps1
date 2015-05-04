param(
	[string]$TargetEnvironment = "Production",
	[Parameter(Mandatory=$true)]
	[string]$Build
)

./MSBuild.bat DataBoss\DataBoss.sln /t:Clean /p:Configuration=Release /v:m
./MSBuild.bat Build.proj "/t:Package" "/p:Configuration=Release;Environment=$TargetEnvironment" /p:Build=$Build /v:m

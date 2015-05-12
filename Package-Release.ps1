$Build = [Regex]::Match([IO.File]::ReadAllText("DataBoss\Properties\Version.cs"), "AssemblyVersion\(\""(?<build>.+)\""\)").Groups["build"].Value

./MSBuild.bat DataBoss\DataBoss.sln /t:Clean /p:Configuration=Release /v:m
./MSBuild.bat Build.proj "/t:Package" "/p:Configuration=Release" /p:Build=$Build /v:m

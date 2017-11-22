@echo off
pushd DataBoss.Specs
dotnet conesole .\..\Build\DataBoss.Specs\Debug\net45\DataBoss.Specs.dll %*
popd
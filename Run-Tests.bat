@echo off
pushd DataBoss.Specs
dotnet conesole -- --multicore %*
popd
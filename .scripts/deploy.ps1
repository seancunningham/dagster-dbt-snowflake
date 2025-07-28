Write-Host("`BUILDING DBT")

$dbt_path = ".\dbt"
# echo "cleaning target"
# $log = uv run --env-file .env dbt clean --project-dir $dbt_path --no-clean-project-files-only
# echo "installing dependancies"
# $log = uv run --env-file .env dbt deps --project-dir $dbt_path
echo "parsing manifest"
$log = uv run --env-file .env dbt parse --project-dir $dbt_path --profiles-dir $dbt_path --target prod

foreach($line in $log){
    if ($line.Contains("Error") -eq $true) {
        echo "DBT BUILD FAILED:"
        return $log
    }
}


Write-Host("`nBUILDING DOCKER IMAGE")
$branch_id = (-join ((97..122) | Get-Random -Count 15 | ForEach-Object {[char]$_}))
$new_image = "dagster/data-platform:"+$branch_id
uv run --env-file .env docker build . `
    --target data_platform -t $new_image

Write-Host("`nDEPLOYING BUILD")
$values = Get-Content -Path .\.scripts\helm_template.yaml
$values = $values -replace "{{ branch_id }}", $branch_id
$values | Out-File -FilePath .\.scripts\helm_values.yaml
helm upgrade --install --hide-notes dagster dagster/dagster `
    -f .\.scripts\helm_values.yaml

Write-Host("`nCLEANING KUBERNETES")
kubectl delete pod --field-selector=status.phase==Succeeded
kubectl delete pod --field-selector=status.phase==Failed
kubectl delete pod --field-selector=status.phase==Completed
kubectl delete pod --field-selector=status.phase==ErrImagePull
kubectl delete pod --field-selector=status.phase==ImagePullBackoff

Write-Host("`nCLEANING DOCKER REPOSITORY")

$old_image=docker inspect data-platform | ConvertFrom-Json
$old_image=$old_image.Config.Image

docker stop data-platform
docker rm data-platform

docker container create -t --name data-platform $new_image

docker stop data-platform-rollback
docker rm data-platform-rollback
docker container create -t --name data-platform-rollback $old_image

Start-Sleep 5
docker image prune -a --force

Write-Host("`nDEPLOYMENT COMPLETE")
Write-Host("Branch ID: " + $branch_id + "`n")

echo "creating dbt state manifest"
$defer_path = $dbt_path+"\state\"
if (!(Test-Path -Path $defer_path -PathType Container)) {
    New-Item -Path $defer_path -ItemType Directory
}
Copy-Item -Path $dbt_path"\target\manifest.json" -Destination $defer_path -Force

echo "CREATING DBT DOCS"
uv run --env-file .env dbt docs generate --project-dir $dbt_path --target prod
echo "CREATING DAGSTER DOCS"
uv run --env-file .env pdoc --html data_platform --force --skip-errors
Remove-Item -Path ./docs -Recurse
Rename-Item -Path ./html -NewName docs

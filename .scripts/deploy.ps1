Write-Host("`BUILDING DBT")

$dbt_path = ".\dbt"
echo "cleaning target"
$log = uv run dbt clean --project-dir $dbt_path --no-clean-project-files-only
echo "installing dependancies"
$log = uv run dbt deps --project-dir $dbt_path
echo "parsing manifest"
$log = uv run dbt parse --project-dir $dbt_path --profiles-dir $dbt_path --target prod

echo "creating deferal manifest"
foreach($line in $log){
    if ($line.Contains("Error") -eq $true) {
        echo "DBT BUILD FAILED:"
        return $log
    }
}

$defer_path = $dbt_path+"\artifacts_prod\"
if (!(Test-Path -Path $defer_path -PathType Container)) {
    New-Item -Path $defer_path -ItemType Directory
}
Copy-Item -Path $dbt_path"\target\manifest.json" -Destination $defer_path -Force

echo "creating docs"
$log = uv run dbt docs generate --project-dir $dbt_path --target prod



Write-Host("`nBUILDING DOCKER IMAGE")
$branch_id = (-join ((97..122) | Get-Random -Count 15 | ForEach-Object {[char]$_}))
$new_image = "dagster/elt-core:"+$branch_id
docker build . --target dagster_elt_core -t $new_image

Write-Host("`nDEPLOYING BUILD")
$values = Get-Content -Path .\.scripts\helm_template.yaml
$values = $values -replace "{{ branch_id }}", $branch_id
$values | Out-File -FilePath .\.scripts\helm_values.yaml
helm upgrade --install --hide-notes dagster dagster/dagster -f .\.scripts\helm_values.yaml

Write-Host("`nCLEANING KUBERNETES")
kubectl delete pod --field-selector=status.phase==Succeeded
kubectl delete pod --field-selector=status.phase==Failed
kubectl delete pod --field-selector=status.phase==Completed
kubectl delete pod --field-selector=status.phase==ErrImagePull
kubectl delete pod --field-selector=status.phase==ImagePullBackoff

Write-Host("`nCLEANING DOCKER REPOSITORY")

$old_image=docker inspect dagster-elt-core | ConvertFrom-Json
$old_image=$old_image.Config.Image

docker stop dagster-elt-core
docker rm dagster-elt-core
docker container create -t --name dagster-elt-core $new_image

docker stop dagster-elt-core-rollback
docker rm dagster-elt-core-rollback
docker container create -t --name dagster-elt-core-rollback $old_image

Start-Sleep 5
docker image prune -a --force

Write-Host("`nDEPLOYMENT COMPLETE")
Write-Host("Branch ID: " + $branch_id + "`n")

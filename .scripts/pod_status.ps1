[Console]::CursorVisible = $false
while($true) {
    $buffer = kubectl get pods
    cls
    $buffer
    $i = 0
    while($i -le 5) {
        $buffer = kubectl get pods
        [Console]::SetCursorPosition(0, 0)
        $buffer
        Start-Sleep -Seconds 1
        $i++
    }
}

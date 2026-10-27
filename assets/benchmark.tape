# Records benchmark.gif with https://github.com/charmbracelet/vhs. Run it from
# the repository root with pygrpc-native and grpcio installed:
#   vhs assets/benchmark.tape

Output assets/benchmark.gif

Set Shell "bash"
Set FontSize 20
Set Width 1240
Set Height 300
Set Padding 28
Set Theme "GitHub Dark"
Set TypingSpeed 35ms
Set WindowBar Colorful

Hide
Type "export DUR=3 PS1='$ ' && clear"
Enter
Show

Type "# an empty unary handler, 8 client processes x 4 threads"
Enter
Sleep 600ms
Type "python benchmarks/echo.py sync 8 4          # grpc.server()"
Enter
Sleep 6.5s
Type "python benchmarks/echo.py native-sync 8 4   # pygrpc_native.server()"
Enter
Sleep 9.5s

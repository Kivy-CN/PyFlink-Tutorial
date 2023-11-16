import sys
from io import StringIO

# 重定向sys.stdout到一个io.StringIO对象
output = StringIO()
sys.stdout = output

# 在这里运行您的代码，所有的stdout输出都会被捕获
print("Hello, world!")

# 恢复sys.stdout
sys.stdout = sys.__stdout__

# 输出捕获的内容
print(output.getvalue())

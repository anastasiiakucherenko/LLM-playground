
import platform
import time

print("Hello from Python inside a Slurm job!")
print("Running on:", platform.node())
print("Python version:", platform.python_version())

# Simulate a task
for i in range(5):
    print(f"Step {i+1}/5")
    time.sleep(1)

print("Done!")

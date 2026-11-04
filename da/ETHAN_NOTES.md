INHERITANCE
The parameters for the child class and the parent class need to have the same names. 
In python this is not necessary!

class A(process):
  def setup(a1):      # change this to a3 and it won't work.
    self.a1 = a1
  def run():
    pass
  def some_method():
    a1 = a1 + 1
    output(a1)

class B(A, process):
  def setup(a1, a2):
    super().setup(a1)
  def run():
    super().some_method()
    output(a1, a2)

def main():
  b = new(B, num= 1)
  setup(b, (10, 20))
  start(b)
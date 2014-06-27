print("b1!")

print("b2!")

class bb:
    def func(self):
        print('bb')

from a import aa

xb = aa()
xb.func()

print('end b')
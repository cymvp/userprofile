#import b
print('main:' + __name__)
print("a1!")
import b
print("a2!")
class aa:
    def func(self):
        print('aa')
print("a3!")
xa = b.bb()

print('main:' + __name__)

if __name__ == "__main__":
    print('!!!')
    xa.func()
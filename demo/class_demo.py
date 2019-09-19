
# Demo 1: 注意 Property, __ 私有变量 和 super 超类方法的调用
class animal():

    def __init__(self, height=100, weight=50):
        self.__height = height
        self.__weight = weight

    @property
    def get_height(self):
        return self.__height

    @property
    def get_weight(self):
        return self.__weight


class dog1(animal):

    def __init__(self):
        super(dog1, self).__init__(height=150, weight=100)
        self.bark = 'Woo Woo!'

    def bark(self):
        print(self.bark)
        return

    def __bark(self):
        print("Private", self.bark)
        return


class dog2(dog1):

    def __init__(self):
        super(dog2, self).__init__()
        self.bark = 'Woo!'

    def bark(self):
        print(self.bark)
        return


# -----
ins1 = dog1()
ins2 = dog2()


# Demo 2: 多重继承中超类方法的特性 -- 自顶而下，不重复
# 注意超类方法初始化的顺序与继承时传入参数的顺序有关 -- 倒序
class A(object):
    def __init__(self, a):
        print('init A...')
        self.a = a

class B(A):
    def __init__(self, a):
        super(B, self).__init__(a)
        print('init B...')

class C(A):
    def __init__(self, a):
        super(C, self).__init__(a)
        print('init C...')

class D(B, C):
    def __init__(self, a):
        super(D, self).__init__(a)
        print('init D...')

class D1(C, B):
    def __init__(self, a):
        super(D1, self).__init__(a)
        print('init D1...')


ins = D(a=10)
print('\n')
inss = D1(a=10)



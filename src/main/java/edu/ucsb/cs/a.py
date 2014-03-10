import math

def prime_factorize(x):
  isPrime=True
  l=[]
  if x<=1:
    return []
  if x==2:
    return [2]
  for i in range(2,x):
    if x%i==0:
      isPrime=False
      l+=[i]
      l+=prime_factorize(x%i)
      l+=prime_factorize(i)
      break
  if isPrime:
    return [x]
  return l

print(prime_factorize(96))

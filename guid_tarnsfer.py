import uuid

id = '\\217\\274\\372\\026>\\334\\017\\016\\021\\357\\226\\310\\301\\331\\352'

values = [hex(int(x[:3], base=8)) for x in id.split('\\') if x] 
print(values)

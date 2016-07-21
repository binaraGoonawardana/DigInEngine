__author__ = 'Manura Omal Bhagya'

import numpy as np
import math
import matplotlib.pyplot as plt

#data = np.matrix([[2009,1,516302.9595],[2009,2,332480.6365],[2009,3,411628.728999999],[2009,4,393276.482],[2009,5,230145.537999999],[2009,6,263456.067999999],[2009,7,380503.97],[2009,8,329754.715],[2009,9,325292.3145],[2009,10,361555.2665],[2009,11,248933.425999999],[2009,12,415809.3505],[2010,1,336526.6805],[2010,2,271580.508],[2010,3,217808.006499999],[2010,4,266968.588999999],[2010,5,283534.284999999],[2010,6,293080.664999999],[2010,7,229885.4985],[2010,8,207937.009],[2010,9,418343.278499999],[2010,10,365251.985],[2010,11,290670.3455],[2010,12,368093.954],[2011,1,251467.228],[2011,2,299890.141],[2011,3,296035.871],[2011,4,288213.397],[2011,5,262628.496],[2011,6,197740.8455],[2011,7,287905.1865],[2011,8,274578.4795], [2011,9,276049.796],[2011,10,305660.451],[2011,11,367769.496],[2011,12,328877.314499999],[2012,1,340626.507],[2012,2,276132.499],[2012,3,348208.325],[2012,4,268024.97],[2012,5,384588.0615],[2012,6,276580.935499999],[2012,7,242809.6995],[2012,8,302745.123499999],[2012,9,318271.566499999],[2012,10,351246.7325],[2012,11,256020.103999999],[2012,12,354709.338]])

def holt_winter(data):

    a = 0.3
    b = 0.01
    ncol = data.shape[1]-1
    # Total Sales
    x = data[:,ncol]
    x = x.astype(float)
    w1 = -0.0758;w2 = 0.3033;w3 = 0.5450;w4 = 0.3033;w5 = -0.0758
    # i =2
    # y = (x[0]*w1+x[1]*w2+x[2]*w3+x[3]*w4+x[4]*w5)
    # print y
    dim = x.shape
    q = dim[0]
    n = q-2

    # Moving average matrix
    mavg = np.zeros(dim)

    # Seasonality matrix
    dimp = dim[0] + 12
    #12 is for forecasting steps
    p = np.zeros((dimp,1))

    # Level Matrix
    m = np.zeros((q,1))
    # Error matrix
    e = np.zeros((q,1))
    # trend matrix
    r = np.zeros((q,1))
    # func matrix
    f = np.zeros((dimp,1))
    # temp matrix
    stemp = np.zeros((q,1))

    # initialize moving averages
    mavg[0] = (0.5450*x[0]+0.3033*x[1]-0.0758*x[2])/0.7725
    mavg[1] = (0.3033*x[0]+0.5450*x[1]+0.3033*x[2]-0.0758*x[3])/1.0758

    mavg[q-1] = (0.5450*x[q-1]+0.3033*x[n]-0.0758*x[n-1])/0.7725
    mavg[n] = (0.3033*x[q-1]+0.5450*x[n]+0.3033*x[n-1]-0.0758*x[n-2])/1.0758

    # Calc moving average
    for i in range(2,n):
        mavg[i,0] = (x[i-2]*w1+x[i-1]*w2+x[i]*w3+x[i+1]*w4+x[i+2]*w5)

    deviation = x - mavg
    exact_deviation = (deviation[0:12]+deviation[12:24]+deviation[24:36]+deviation[36:48])/4
    # avg_deviation = (deviation[0:12] + deviation[12:24])/2
    # mean_avg_deviation = sum(avg_deviation)/12
    p_prime = exact_deviation
    ln = int(math.ceil(float(dimp)/12))


    for i in range(0,ln):
        p[i*12:12*(1+i)] = p_prime

    # Initialize m/level
    m[0] = x[0]

    # Initialize trend
    r[0] = (np.mean(x[12:21]) - np.mean(x[0:9]))

    # default k
    k = 1

    for t in range(2,q):
         e[t] = x[t]- (m[t-1]+r[t-1]+p[t])
         m[t] = m[t-1] + r[t-1] + a*e[t]
         r[t] = r[t-1] + b*e[t]
         f[t] = m[t] + r[t] + p[t]

    # print e
    #
    # for k in range(1,14):
    #     f[t+k] = m[t] + k*r[t] + p[t+k]
    #
    # k = k-1
    #mean_et = np.mean(e)
    std_et = np.std(e)
    #print std_et
    #
    g = x[k+2:48]/f[k+2:48]

    sort_g = g[np.array(g[:,0].argsort(axis=0).tolist()).ravel()]

    #len_g = len(g)+1
    sort_g = np.squeeze(np.asarray(sort_g))

    #print sort_g.shape

    slen = np.arange(len(sort_g))/float(len(sort_g)-1)
    #print slen.shape


    # print sort_g
    #c_cdf = plt.plot(sort_g,slen)

    #plt.show(c_cdf)

    #print sort_g

    stemp = x
    #
    for t in range(1,(n+1)):
        e[t] = stemp[t]- (m[t-1]+r[t-1]+p[t])

        if e[t] > 2*std_et:

            stemp[t] = f[t] + std_et
            e[t] = stemp[t] - (m[t-1]+r[t-1]+p[t])
            m[t] = m[t-1] + r[t-1] + a*e[t]
            r[t] = r[t-1] + b*e[t]

        else:

            m[t] = m[t-1] + r[t-1] + a*e[t]
            r[t] = r[t-1] + b*e[t]
    for k in range(1,14):
        f[t+k] = m[t] + k*r[t] + p[t+k]
    # TODO automate finding x_lower n x_upper
    x_lower = 0.786
    x_upper = 1.122

    f_lower = f * x_lower
    f_upper = f * x_upper

    # print f_lower,f,f_upper
    # print type(f)

    f = np.append(f,f_lower,axis=1)
    f= np.append(f,f_upper,axis = 1)



    return f

#print holt_winter(data)
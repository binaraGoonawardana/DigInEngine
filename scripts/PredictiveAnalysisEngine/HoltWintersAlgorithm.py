#The MIT License (MIT)
#
#Copyright (c) 2015 Andre Queiroz
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.
#
# Holt-Winters algorithms to forecasting
# Coded in Python 2 by: Andre Queiroz
# Description: This module contains three exponential smoothing algorithms. They are Holt's linear trend method and Holt-Winters seasonal methods (additive and multiplicative).
# References:
#  Hyndman, R. J.; Athanasopoulos, G. (2013) Forecasting: principles and practice. http://otexts.com/fpp/. Accessed on 07/03/2013.
#  Byrd, R. H.; Lu, P.; Nocedal, J. A Limited Memory Algorithm for Bound Constrained Optimization, (1995), SIAM Journal on Scientific and Statistical Computing, 16, 5, pp. 1190-1208.

from __future__ import division
from sys import exit
from math import sqrt
from numpy import array
from scipy.optimize import fmin_l_bfgs_b

def RMSE(params, *args):

	Y = args[0]
	type = args[1]
	rmse = 0

	if type == 'Linear':

		alpha, beta = params
		a = [Y[0]]
		b = [Y[1] - Y[0]]
		y = [a[0] + b[0]]

		for i in range(len(Y)):

			a.append(alpha * Y[i] + (1 - alpha) * (a[i] + b[i]))
			b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
			y.append(a[i + 1] + b[i + 1])

	else:

		alpha, beta, gamma = params
		m = args[2]		
		a = [sum(Y[0:m]) / float(m)]
		b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]

		if type == 'Additive':

			s = [Y[i] - a[0] for i in range(m)]
			y = [a[0] + b[0] + s[0]]

			for i in range(len(Y)):

				a.append(alpha * (Y[i] - s[i]) + (1 - alpha) * (a[i] + b[i]))
				b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
				s.append(gamma * (Y[i] - a[i] - b[i]) + (1 - gamma) * s[i])
				y.append(a[i + 1] + b[i + 1] + s[i + 1])

		elif type == 'Multiplicative':

			s = [Y[i] / a[0] for i in range(m)]
			y = [(a[0] + b[0]) * s[0]]

			for i in range(len(Y)):

				a.append(alpha * (Y[i] / s[i]) + (1 - alpha) * (a[i] + b[i]))
				b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
				s.append(gamma * (Y[i] / (a[i] + b[i])) + (1 - gamma) * s[i])
				y.append((a[i + 1] + b[i + 1]) * s[i + 1])

		else:

			exit('Type must be either linear, additive or multiplicative')
		
	rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y, y[:-1])]) / len(Y))

	return rmse

def linear(x, fc, alpha = None, beta = None):

	Y = x[:]

	if (alpha == None or beta == None):

		initial_values = array([0.3, 0.1])
		boundaries = [(0, 1), (0, 1)]
		type = 'Linear'

		parameters = fmin_l_bfgs_b(RMSE, x0 = initial_values, args = (Y, type), bounds = boundaries, approx_grad = True)
		alpha, beta = parameters[0]

	a = [Y[0]]
	b = [Y[1] - Y[0]]
	y = [a[0] + b[0]]
	rmse = 0

	for i in range(len(Y) + fc):

		if i == len(Y):
			Y.append(a[-1] + b[-1])

		a.append(alpha * Y[i] + (1 - alpha) * (a[i] + b[i]))
		b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
		y.append(a[i + 1] + b[i + 1])

	rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y[:-fc], y[:-fc - 1])]) / len(Y[:-fc]))

	return Y[-fc:], alpha, beta, rmse

def additive(x, m, fc, alpha = None, beta = None, gamma = None):

	Y = x[:]

	if (alpha == None or beta == None or gamma == None):

		initial_values = array([0.3, 0.1, 0.1])
		boundaries = [(0, 1), (0, 1), (0, 1)]
		type = 'Additive'

		parameters = fmin_l_bfgs_b(RMSE, x0 = initial_values, args = (Y, type, m), bounds = boundaries, approx_grad = True)
		alpha, beta, gamma = parameters[0]

	a = [sum(Y[0:m]) / float(m)]
	b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]
	s = [Y[i] - a[0] for i in range(m)]
	y = [a[0] + b[0] + s[0]]
	rmse = 0

	for i in range(len(Y) + fc):

		if i == len(Y):
			Y.append(a[-1] + b[-1] + s[-m])

		a.append(alpha * (Y[i] - s[i]) + (1 - alpha) * (a[i] + b[i]))
		b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
		s.append(gamma * (Y[i] - a[i] - b[i]) + (1 - gamma) * s[i])
		y.append(a[i + 1] + b[i + 1] + s[i + 1])

	rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y[:-fc], y[:-fc - 1])]) / len(Y[:-fc]))

	return Y[-fc:], alpha, beta, gamma, rmse

def multiplicative(x, m, fc, alpha = None, beta = None, gamma = None):

	Y = x[:]

	if (alpha == None or beta == None or gamma == None):

		initial_values = array([0.0, 1.0, 0.0])
		boundaries = [(0, 1), (0, 1), (0, 1)]
		type = 'Multiplicative'

		parameters = fmin_l_bfgs_b(RMSE, x0 = initial_values, args = (Y, type, m), bounds = boundaries, approx_grad = True)
		alpha, beta, gamma = parameters[0]

	a = [sum(Y[0:m]) / float(m)]
	b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]
	s = [Y[i] / a[0] for i in range(m)]
	y = [(a[0] + b[0]) * s[0]]
	rmse = 0

	for i in range(len(Y) + fc):

		if i == len(Y):
			Y.append((a[-1] + b[-1]) * s[-m])

		a.append(alpha * (Y[i] / s[i]) + (1 - alpha) * (a[i] + b[i]))
		b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
		s.append(gamma * (Y[i] / (a[i] + b[i])) + (1 - gamma) * s[i])
		y.append((a[i + 1] + b[i + 1]) * s[i + 1])

	rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y[:-fc], y[:-fc - 1])]) / len(Y[:-fc]))

	return Y[-fc:], alpha, beta, gamma, rmse

#data = [516302.9595000003, 332480.6365, 411628.72899999993, 393276.4820000002, 230145.53799999997, 263456.06799999997, 380503.97000000003, 329754.715, 325292.3145000001, 361555.26650000014, 248933.42599999995, 415809.3505000001, 336526.6805000001, 271580.50800000003, 217808.00649999996, 266968.5889999999, 283534.2849999998, 293080.6649999999, 229885.49850000005, 207937.00900000002, 418343.27849999996, 365251.985, 290670.34550000005, 368093.95400000014, 251467.22800000006, 299890.141, 296035.8710000001, 288213.3970000002, 262628.4960000001, 197740.84550000002, 287905.18650000007, 274578.4795, 276049.796, 305660.451, 367769.4960000002, 328877.3144999999, 340626.5070000001, 276132.49900000007, 348208.3250000001, 268024.97000000003, 384588.06150000036, 276580.93549999996, 242809.69950000013, 302745.1234999999, 318271.5664999997, 351246.73250000016, 256020.10399999996, 354709.3380000001]
#print RMSE([0.3,0.1,0.1],data,'linear',7)

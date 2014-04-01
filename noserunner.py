import nose

# this script is intended to be run by jython,
# so we have the java and python dependencies available
if __name__ == '__main__':
    nose.main(argv=['paging_test.py', '--with-xunit'])

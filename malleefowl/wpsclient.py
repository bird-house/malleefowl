def main():
    import optparse

    parser = optparse.OptionParser()
    parser.add_option('-v', '--verbose',
                      dest="verbose",
                      default=False,
                      action="store_true",
                      )

    options, remainder = parser.parse_args()
    
    print 'VERBOSE   :', options.verbose


class ConfigValidationError(RuntimeError):
    """ Raised when a parsed configuration does not pass validation"""
    pass

class NoDiscoveryException(RuntimeError):
    """
        An exception which indicates that a discovery atom yeilded no data.
        Should trigger the whole subsequent pipeline to just gracefully finish.
    """

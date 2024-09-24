import os, functools, time

def find_files_in_folder(folder, extension):
    if os.path.exists(folder):
        paths= []
        for file in os.listdir(folder):
            if file.endswith(extension):
                paths.append(os.path.join(folder , file))

        return paths
    else:
        raise Exception("path does not exist -> "+ folder)
    


def get_size_mb(path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return int(total_size / (1024 * 1024))


def timer(logger=None):
    """Print the runtime of the decorated function"""
    def decorator_timer(func):
        @functools.wraps(func)
        def wrapper_timer(*args, **kwargs):
            start_time = time.perf_counter()    # 1
            value = func(*args, **kwargs)
            end_time = time.perf_counter()      # 2
            run_time = round((end_time - start_time),4)    # 3
            msg = "Finished " + str(func.__name__) + " in "+ str(run_time) + " secs"
            if logger != None:
                logger.info(msg)
            else:
                print(msg)
            return value
        return wrapper_timer
    return decorator_timer

def try_except(logger=None):
    """Wrap the decorated function with exception catch with optional logger"""
    def decorator_try_except(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try: 
                return func(*args, **kwargs)
            except:
                msg = "from >> "+str(func.__name__)
                if logger != None:
                    logger.exception(msg)
                else:
                    print(msg)
                return False
        return wrapper
    return decorator_try_except

from setuptools import setup, find_packages

HYPHEN_E_DASH = '-e .'

def get_requirements(file_path):
    """
    This function returns a list of requirements from the given file path.
    """
    with open(file_path) as file:
        lines = file.readlines()
    
    # Strip and filter
    requirements = []
    for line in lines:
        line = line.strip()
        if line and line != HYPHEN_E_DASH:
            requirements.append(line)
    
    return requirements


setup(
    name='NetworkSecurity',
    version='0.0.1',
    author='Aayush',
    author_email='aayushkr646@gmail.com',
    packages=find_packages(),
    install_requires= get_requirements('requirements.txt')

)
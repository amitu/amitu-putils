from setuptools import setup, find_packages

setup(
    name = "amitu-putils",
    description = "Python Utilities",
    version = "0.1",
    author = 'Amit Upadhyay',
    author_email = "upadhyay@gmail.com",

    url = 'http://github.com/amitu/amitu-putils/',
    license = 'BSD',

    namespace_packages = ["amitu"],
    packages = find_packages(),

    install_requires = [],
    zip_safe = True,

    entry_points={
        'console_scripts': [
        ]
    },
)

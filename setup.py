from setuptools import find_packages, setup

setup(
    name="py-dagger",
    version="2.0.0rc6",
    description="Workflow Engine",
    author="Vikram Patki, Aditya Vaderiyattil",
    author_email="vpatki@wayfair.com, avaderiyattil@wayfair.com",
    url="https://github.com/wayfair-incubator/dagger",
    platforms=["any"],
    license="MIT License",
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=["faust-streaming"],
    python_requires="~=3.7",
    entry_points={"console_scripts": ["dagger = dagger.dagger:main"]},
)

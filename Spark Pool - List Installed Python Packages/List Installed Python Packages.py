import pkg_resources

installed_packages = pkg_resources.working_set
for package in sorted([f"{i.key}=={i.version}" for i in installed_packages]):
    print(package)
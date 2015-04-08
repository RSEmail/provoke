Name:           provoke
Version:        0.0.0
Release:        1
Summary:        Lightweight, asynchronous function execution in Python using AMQP.
Group:          Applications/System

License:        MIT
URL:            https://github.com/icgood/provoke
Source:         %{name}-%{version}.tar.gz
Source1:        %{name}.init
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-buildroot

BuildArch:      noarch
BuildRequires:  python, python-setuptools
Requires(post): chkconfig
Requires(preun): chkconfig, initscripts

Requires:       python, python-setuptools

%description
Lightweight, asynchronous function execution in Python using AMQP.

%prep
%setup -q -n %{name}-%{version}

%build
%{__python} setup.py build

%install
%{__python} setup.py install --skip-build -O1 --record=files.txt --single-version-externally-managed --root %{buildroot}
install -m 755 %{SOURCE1} %{buildroot}/%{_initddir}/%{name}

%post
/sbin/chkconfig --add %{name}

%preun
if [ $1 -eq 0 ] ; then
    /sbin/service %{name} stop >/dev/null 2>&1
    /sbin/chkconfig --del %{name}
fi

%files -f files.txt
%defattr(-,root,root,-)
%{_initddir}/%{name}

%changelog
* Wed Apr 08 2015 Ian Good <icgood@gmail.com> 0.0.0-1
- Initial spec

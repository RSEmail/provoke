
%{?el5: %define py_basever 26}
%{?el5: %define __python /usr/bin/python2.6}
%{?el5: %define _initddir %{_initrddir}}

Name:           provoke
Version:        0.0.0
Release:        1
Summary:        Lightweight, asynchronous function execution in Python using AMQP.
Group:          Applications/System

License:        MIT
URL:            https://github.com/icgood/provoke
Source:         provoke-%{version}.tar.gz
Source1:        provoke@.service
Source2:        provoke.init
BuildRoot:      %{_tmppath}/provoke-%{version}-%{release}-buildroot

BuildArch:      noarch
BuildRequires:  python%{?py_basever}, python%{?py_basever}-setuptools
%if 0%{?rhel} >= 7
BuildRequires:  systemd
%else
Requires(post): chkconfig
Requires(preun): chkconfig, initscripts
%endif

Requires:       python%{?py_basever}, python%{?py_basever}-setuptools

%description
Lightweight, asynchronous function execution in Python using AMQP.

%prep
%setup -q -n provoke-%{version}

%build
%{__python} setup.py build

%install
%{__python} setup.py install --skip-build -O1 --record=files.txt --single-version-externally-managed --root %{buildroot}

%if 0%{?rhel} >= 7
mkdir -p %{buildroot}/%{_unitdir}
install -m 644 %{SOURCE1} %{buildroot}/%{_unitdir}/provoke@.service
%else
mkdir -p %{buildroot}/%{_initddir}
install -m 755 %{SOURCE2} %{buildroot}/%{_initddir}/provoke
%endif

%post
%if ! 0%{?rhel} >= 7
/sbin/chkconfig --add <script>
%endif

%preun
%if ! 0%{?rhel} >= 7
if [ $1 -eq 0 ] ; then
    /sbin/service provoke stop >/dev/null 2>&1
    /sbin/chkconfig --del provoke
fi
%endif

%files -f files.txt
%defattr(-,root,root,-)
%if 0%{?rhel} >= 7
%{_unitdir}/provoke@.service
%else
%{_initddir}/provoke
%endif

%changelog
* Sat Jul 25 2015 Ian Good <icgood@gmail.com> 0.0.0-1
- Support all current RHELs

* Wed Apr 08 2015 Ian Good <icgood@gmail.com> 0.0.0-1
- Initial spec

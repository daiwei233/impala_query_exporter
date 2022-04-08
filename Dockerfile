FROM python:3.7

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo 'Asia/Shanghai' >/etc/timezone

WORKDIR /code
COPY . /code
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt --proxy http://10.18.34.194:3128 -i https://pypi.doubanio.com/simple/
CMD ["python", "main.py"]
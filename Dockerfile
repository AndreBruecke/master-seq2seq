FROM tensorflow/tensorflow:latest-gpu
WORKDIR /thesis
COPY . .
RUN pip install --upgrade pip
RUN pip install --upgrade -r requirements.txt
EXPOSE 8888
ENTRYPOINT ["jupyter", "lab","--ip=0.0.0.0","--allow-root","--no-browser"]
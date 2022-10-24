FROM ubuntu

# RUN mkdir -p /inputs
# RUN mkdir -p /outputs
# RUN echo "100" > /inputs/0.txt
# RUN echo "100" > /inputs/00.txt

CMD cat /inputs/*.txt | awk '{n += $1}; END{print n}' > /outputs/sum.txt && cat /outputs/sum.txt
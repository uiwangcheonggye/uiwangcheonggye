## Hi there 👋

<!--
**uiwangcheonggye/uiwangcheonggye** is a ✨ _special_ ✨ repository because its `README.md` (this file) appears on your GitHub profile.

Here are some ideas to get you started:

- 🔭 I’m currently working on ...
- 🌱 I’m currently learning ...
- 👯 I’m looking to collaborate on ...
- 🤔 I’m looking for help with ...
- 💬 Ask me about ...
- 📫 How to reach me: ...
- 😄 Pronouns: ...
- ⚡ Fun fact: ...
-->

~~~mermaid
graph TD

subgraph 사용자 입력
    A[📝 사용자] -->|폼 작성| B[Google Form]
end

subgraph Google 시스템
    B -->|자동 저장| C[Google Sheet<br>Form Responses 시트]
end

subgraph GitHub Actions
    C -->|Pull + Mapping| D[📦 GitHub Action 스크립트]
    D -->|병합 및 업데이트, 동,호수 단위 | E[관리용 Google Sheet]
end

%% 스케줄러 트리거
subgraph 자동 트리거
    F[🕘 매일 오전 9시] --> D
end

style B fill:#cfe2ff,stroke:#0056b3,color:#000
style C fill:#d1e7dd,stroke:#146c43,color:#000
style E fill:#d1e7dd,stroke:#146c43,color:#000
style D fill:#fff3cd,stroke:#997404,color:#000
style F fill:#f8d7da,stroke:#842029,color:#000
~~~

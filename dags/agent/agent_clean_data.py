import os
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.agents import Tool, initialize_agent, AgentType
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
GPT_MODEL = os.getenv("GPT_MODEL")

# Cấu hình OpenAI API Key
llm = ChatOpenAI(model=GPT_MODEL, temperature=0)

# Định nghĩa Prompt làm sạch dữ liệu
cleaning_prompt = PromptTemplate(
    input_variables=["raw_content"],
    template="""
    Bạn là một chuyên gia xử lý dữ liệu. Nhiệm vụ của bạn là làm sạch nội dung văn bản phi cấu trúc.
    Hãy loại bỏ tất cả thông tin không cần thiết như footer và các thông tin liên hệ cá nhân, nhưng phải giữ nguyên toàn bộ nội dung chính, không được tóm tắt hoặc thay đổi độ dài nội dung.
    Lưu ý:Kết quả trả về phải là tiếng việt.
    Dữ liệu đầu vào:
    {raw_content}

    Hãy trả toàn bộ nội dung ngoại trừ các thông tin trong footer và các thông tin liên hệ như email,số điện thoại,địa chỉ,...:
    """
)

# Tạo một LLMChain với Prompt
cleaning_chain = LLMChain(llm=llm, prompt=cleaning_prompt)

# Tích hợp công cụ làm sạch vào Agent
cleaning_tool = Tool(
    name="DataCleaner",
    func=cleaning_chain.run,
    description="loại bỏ tất cả thông tin không cần thiết như footer và các thông tin liên hệ cá nhân, nhưng phải giữ nguyên toàn bộ nội dung chính, không được tóm tắt hoặc thay đổi độ dài nội dung.Nội dung trả về là Tiếng Việt"
)

# Tạo Agent
tools = [cleaning_tool]
agent_clean_data = initialize_agent(
    tools=tools, 
    llm=llm, 
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, 
    verbose=True, 
    max_iterations=10, 
    early_stopping_method="generate"
)

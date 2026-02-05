"""
LLM-powered insights endpoints
Natural language explanations and conversational queries
"""
from fastapi import APIRouter, HTTPException
from typing import Dict, Optional
from pydantic import BaseModel

from app.services.llm_service import LLMService
from app.services.chat_data_service import ChatDataService
from app.utils.logger import log

router = APIRouter(prefix="/llm", tags=["llm"])

llm_service = LLMService()


class QuestionRequest(BaseModel):
    question: str
    context_data: Dict


class ChatRequest(BaseModel):
    message: str


class ExplainAnomalyRequest(BaseModel):
    anomaly: Dict


class GenerateEmailRequest(BaseModel):
    customer: Dict


class ExplainRecommendationsRequest(BaseModel):
    recommendations: list


@router.get("/status")
async def get_llm_status():
    """Check if LLM service is available"""
    return {
        "available": llm_service.is_available(),
        "message": "LLM service is ready" if llm_service.is_available() else "LLM service not configured"
    }


@router.post("/chat")
async def chat(request: ChatRequest):
    """
    Simple chat endpoint - automatically loads context from DATABASE.

    All data comes from the database - no API calls for historical questions.
    Only uses real-time API for "right now" or "live" questions.

    Example: {"message": "What was our revenue last 30 days?"}
    """
    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env"
        )

    try:
        # Load context from DATABASE
        chat_data = ChatDataService()

        # Check if this needs real-time API (only for "right now" questions)
        if chat_data.is_realtime_question(request.message):
            # Very rare - only for live/real-time questions
            context = {
                'data_source': 'REALTIME_API',
                'note': 'This is live data from APIs',
                'database_stats': chat_data.get_database_stats()
            }
            context_type = "realtime"
        else:
            # DEFAULT: Use DATABASE for everything
            context = chat_data.get_context_for_question(request.message)
            context_type = "database"

        # DEBUG: Log context keys to verify WoW data is included
        log.info(f"Chat context keys: {list(context.keys())}")
        if 'SEARCH_CONSOLE_WOW' in context:
            wow_data = context['SEARCH_CONSOLE_WOW']
            log.info(f"WoW data present: ctr_gainers={len(wow_data.get('ctr_gainers', []))}, "
                     f"current_period={wow_data.get('current_period', {}).get('label')}")
        else:
            log.info("SEARCH_CONSOLE_WOW NOT in context")

        # Get answer from LLM
        answer = llm_service.answer_question(
            question=request.message,
            context_data=context
        )

        # Include data summary in response
        db_stats = context.get('database_stats', {})
        orders_info = db_stats.get('orders', {})

        return {
            "message": request.message,
            "response": answer,
            "context_type": context_type,
            "data_source": {
                "type": "DATABASE",
                "orders_count": orders_info.get('count', 0),
                "date_range": orders_info.get('date_range', 'N/A'),
                "total_revenue": orders_info.get('total_revenue', 0)
            }
        }

    except Exception as e:
        log.error(f"Error in chat: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ask")
async def ask_question(request: QuestionRequest):
    """
    Ask a natural language question about your data

    Example questions:
    - "Why did my revenue drop last week?"
    - "Which customers are most likely to churn?"
    - "What's wrong with my Google Ads campaigns?"
    - "How can I improve my conversion rate?"
    """
    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env"
        )

    try:
        answer = llm_service.answer_question(
            question=request.question,
            context_data=request.context_data
        )

        return {
            "question": request.question,
            "answer": answer,
            "timestamp": "now"
        }

    except Exception as e:
        log.error(f"Error answering question: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/explain/anomaly")
async def explain_anomaly(request: ExplainAnomalyRequest):
    """
    Get AI-powered explanation for a detected anomaly
    """
    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available"
        )

    try:
        explanation = llm_service.explain_anomaly(request.anomaly)

        return {
            "anomaly": request.anomaly,
            "explanation": explanation
        }

    except Exception as e:
        log.error(f"Error explaining anomaly: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/explain/recommendations")
async def explain_recommendations(request: ExplainRecommendationsRequest):
    """
    Get AI-powered explanation of recommendations
    """
    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available"
        )

    try:
        explanation = llm_service.explain_recommendations(request.recommendations)

        return {
            "total_recommendations": len(request.recommendations),
            "explanation": explanation
        }

    except Exception as e:
        log.error(f"Error explaining recommendations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/generate/email/winback")
async def generate_winback_email(request: GenerateEmailRequest):
    """
    Generate a personalized win-back email for a churning customer
    """
    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available"
        )

    try:
        email = llm_service.generate_win_back_email(request.customer)

        return {
            "customer_email": request.customer.get('email'),
            "generated_email": email
        }

    except Exception as e:
        log.error(f"Error generating email: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/summary")
async def generate_summary(analysis_results: Dict):
    """
    Generate an executive summary from analysis results
    """
    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available"
        )

    try:
        summary = llm_service.generate_executive_summary(analysis_results)

        return {
            "summary": summary,
            "analysis_timestamp": analysis_results.get('timestamp')
        }

    except Exception as e:
        log.error(f"Error generating summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

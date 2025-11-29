from flask import jsonify
from models import Incentive
import logging

logger = logging.getLogger(__name__)

def incentive_report(request):
    """
    Get incentive report data from incentives table.
    Optional query parameters:
    - epoch: Filter by specific epoch
    - protocol: Filter by protocol name (e.g. resupply, yieldbasis)
    - limit: Limit number of results (default: 100)
    - offset: Offset for pagination (default: 0)
    """
    try:
        # Get query parameters
        epoch = request.args.get('epoch', type=int)
        protocol = request.args.get('protocol', type=str)
        limit = request.args.get('limit', default=100, type=int)
        offset = request.args.get('offset', default=0, type=int)
        
        # Validate limit
        if limit > 1000:
            limit = 1000
        if limit < 1:
            limit = 100
            
        # Build query
        query = Incentive.query
        
        # Filter by epoch if provided
        if epoch is not None:
            query = query.filter(Incentive.epoch == epoch)

        # Filter by protocol if provided
        if protocol:
            query = query.filter(Incentive.protocol == protocol)
        
        # Order by timestamp descending (newest first)
        query = query.order_by(Incentive.timestamp.desc())
        
        # Get total count before pagination
        total_count = query.count()
        
        # Apply pagination
        incentives = query.limit(limit).offset(offset).all()
        
        # Convert to dict
        results = [incentive.to_dict() for incentive in incentives]
        
        response = {
            'success': True,
            'data': results,
            'pagination': {
                'total': total_count,
                'limit': limit,
                'offset': offset,
                'returned': len(results)
            }
        }
        
        logger.info(f"Incentive report query returned {len(results)} results (total: {total_count})")
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Error in incentive_report: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'message': str(e)
        }), 500

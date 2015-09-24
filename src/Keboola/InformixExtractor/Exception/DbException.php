<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 04/12/13
 * Time: 14:21
 */

namespace Keboola\DbExtractorBundle\Exception;

class DbException extends \Exception
{
    public function __construct($message = null, \Exception $previous = null)
    {
        parent::__construct($message, 400, $previous);
    }
}
